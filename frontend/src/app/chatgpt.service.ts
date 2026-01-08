import { Injectable } from "@angular/core";

export interface WorkflowContext {
  computeUnits: any;
  datasetName: string;
  datasetTupleCount: number;
  operatorTypes: string[];
  stats: {
    maxCpuUsage: number;
    maxMemUsage: number;
    startCpuUsage: number;
    startMemUsage: number;
    endCpuUsage: number;
    endMemUsage: number;
    avgCpuUsage: number;
    avgMemUsage: number;
  };
}

@Injectable({
  providedIn: "root",
})
export class ChatGptService {
  private readonly apiKey = "";

  async getMultiAgentDecision(context: WorkflowContext): Promise<{ uid: number; explanation: string }> {
    try {
      console.log("Starting Multi-Agent Decision Process...");

      // Parallel execution of Agent 1 (Time) and Agent 2 (Resource)
      const [timeResult, resourceResult] = await Promise.all([
        this.getTimeOptimizationDecision(context),
        this.getResourceUtilizationDecision(context),
      ]);

      console.log("Time Agent Result:", timeResult);
      console.log("Resource Agent Result:", resourceResult);

      // Agent 3 (Aggregator) decides based on the outputs of the first two
      const finalDecision = await this.getAggregatorDecision(timeResult, resourceResult, context);

      console.log("Aggregator Final Decision:", finalDecision);
      return finalDecision;
    } catch (error) {
      console.error("Multi-Agent process failed:", error);
      throw error;
    }
  }

  private async getTimeOptimizationDecision(context: WorkflowContext): Promise<{ uid: number; explanation: string }> {
    const prompt = `
You are the **Time Optimization Agent**. Your sole goal is to minimize the total execution time of the workflow.
Ignore costs and resource efficiency unless they directly impact speed (e.g., thrashing).

**Workflow Context**:
- Dataset: ${context.datasetName} (${context.datasetTupleCount} tuples)
- Operators: ${JSON.stringify(context.operatorTypes)}
- Historical Stats: ${JSON.stringify(context.stats)}

**Available Compute Units**:
${JSON.stringify(context.computeUnits)}

**Instructions**:
1. Identify the compute unit that offers the highest raw performance (CPU speed, core count) suitable for this workflow.
2. Consider data locality if implied by the dataset name or context (though not explicitly provided here, assume standard network).
3. Prioritize units with low current load to ensure immediate availability.

**Output Format**:
Respond with exactly two lines:
Line 1: The UID of the best compute unit for SPEED.
Line 2: A brief explanation focusing ONLY on speed/latency.
`;
    return this.callOpenAI(prompt, "Time Optimization Agent");
  }

  private async getResourceUtilizationDecision(context: WorkflowContext): Promise<{ uid: number; explanation: string }> {
    const prompt = `
You are the **Resource Utilization Agent**. Your sole goal is to optimize for cluster efficiency and resource usage.
You want to prevent over-provisioning (wasting a powerful node on a small task) and under-provisioning (causing failures).

**Workflow Context**:
- Dataset: ${context.datasetName} (${context.datasetTupleCount} tuples)
- Operators: ${JSON.stringify(context.operatorTypes)}
- Historical Stats: ${JSON.stringify(context.stats)}

**Available Compute Units**:
${JSON.stringify(context.computeUnits)}

**Instructions**:
1. Select a compute unit that "fits" the workflow's resource needs without excessive waste.
2. If the workflow is small, choose a smaller or already-utilized node (bin-packing).
3. If the workflow is heavy, ensure the node has enough headroom, but don't pick the absolute largest if a smaller one suffices.

**Output Format**:
Respond with exactly two lines:
Line 1: The UID of the best compute unit for EFFICIENCY.
Line 2: A brief explanation focusing ONLY on resource utilization/efficiency.
`;
    return this.callOpenAI(prompt, "Resource Utilization Agent");
  }

  private async getAggregatorDecision(
    timeResult: { uid: number; explanation: string },
    resourceResult: { uid: number; explanation: string },
    context: WorkflowContext
  ): Promise<{ uid: number; explanation: string }> {
    const prompt = `
You are the **Aggregator Agent**. You make the final decision on which compute unit to use.
You have received recommendations from two specialist agents.

**Workflow Context**:
- Dataset: ${context.datasetName} (${context.datasetTupleCount} tuples)
- Operators: ${JSON.stringify(context.operatorTypes)}

**Agent Recommendations**:
1. **Time Optimization Agent** (Focus: Speed):
   - Recommended UID: ${timeResult.uid}
   - Reason: ${timeResult.explanation}

2. **Resource Utilization Agent** (Focus: Efficiency):
   - Recommended UID: ${resourceResult.uid}
   - Reason: ${resourceResult.explanation}

**Instructions**:
1. Compare the two recommendations.
2. If they agree, output that UID and synthesize the reasons.
3. If they disagree, weigh the trade-offs.
   - For "interactive" or small workflows (low tuple count), prioritize SPEED.
   - For large batch workflows, prioritize EFFICIENCY (unless the Time Agent warns of timeouts).
   - If the Time Agent suggests a node that is critically overloaded (high CPU usage in stats), lean towards the Resource Agent.
4. Make a final, definitive choice.

**Output Format**:
Respond with exactly two lines:
Line 1: The UID of the FINAL selected compute unit.
Line 2: A final explanation justifying the trade-off made.
`;
    return this.callOpenAI(prompt, "Aggregator Agent");
  }

  private async callOpenAI(prompt: string, agentName: string): Promise<{ uid: number; explanation: string }> {
    try {
      const response = await fetch("https://api.openai.com/v1/chat/completions", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${this.apiKey}`,
        },
        body: JSON.stringify({
          model: "gpt-4o",
          messages: [
            { role: "system", content: `You are the ${agentName}.` },
            { role: "user", content: prompt },
          ],
        }),
      });

      if (!response.ok) {
        const errorBody = await response.json();
        throw new Error(`OpenAI API error (${agentName}): ${JSON.stringify(errorBody)}`);
      }

      const result = await response.json();
      const content = result.choices?.[0]?.message?.content?.trim() ?? "";

      const [uidLine, ...explanationLines] = content
        .split("\n")
        .map((line: string) => line.trim())
        .filter(Boolean);

      // Handle potential markdown formatting or extra text
      const uidMatch = uidLine.match(/\d+/);
      const uid = uidMatch ? parseInt(uidMatch[0], 10) : NaN;

      const explanation = explanationLines.join(" ");

      if (isNaN(uid)) {
        throw new Error(`Invalid UID received from ${agentName}: "${uidLine}"`);
      }

      return { uid, explanation };
    } catch (error) {
      console.error(`${agentName} error:`, error);
      throw error;
    }
  }
}
