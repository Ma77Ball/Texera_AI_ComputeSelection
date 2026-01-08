import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { AppSettings } from "../../../../common/app-setting";
import { Observable } from "rxjs";

export interface WorkflowStatistics {
  workflow_id: number;
  execution_id: number;
  cpu_usage_max: number;
  cpu_usage_avg: number;
  cpu_usage_start: number;
  cpu_usage_end: number;
  mem_usage_max: number;
  mem_usage_avg: number;
  mem_usage_start: number;
  mem_usage_end: number;
}

export interface AggregatedWorkflowStatistics {
  maxCpuUsage: number;
  avgCpuUsage: number;
  startCpuUsage: number;
  endCpuUsage: number;
  maxMemUsage: number;
  avgMemUsage: number;
  startMemUsage: number;
  endMemUsage: number;
}

@Injectable({
  providedIn: "root",
})
export class StatisticsService {
  constructor(private http: HttpClient) {}

  public saveWorkflowStats(stats: WorkflowStatistics): Observable<void> {
    return this.http.post<void>(`${AppSettings.getApiEndpoint()}/statistics/workflow`, stats);
  }

  public getAggregatedWorkflowStats(wid: number): Observable<AggregatedWorkflowStatistics> {
    return this.http.get<AggregatedWorkflowStatistics>(`${AppSettings.getApiEndpoint()}/statistics/workflow/${wid}`);
  }
}
