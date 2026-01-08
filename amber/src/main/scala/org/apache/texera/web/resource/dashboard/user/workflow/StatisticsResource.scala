package org.apache.texera.web.resource.dashboard.user.workflow

import io.dropwizard.auth.Auth
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.SqlServer
import org.jooq.impl.DSL
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import scala.jdk.CollectionConverters._

case class WorkflowStatisticsRequest(
    workflow_id: Int,
    execution_id: Int,
    cpu_usage_max: Double,
    cpu_usage_avg: Double,
    cpu_usage_start: Double,
    cpu_usage_end: Double,
    mem_usage_max: Double,
    mem_usage_avg: Double,
    mem_usage_start: Double,
    mem_usage_end: Double
)

case class AggregatedWorkflowStatistics(
    maxCpuUsage: Double,
    avgCpuUsage: Double,
    startCpuUsage: Double,
    endCpuUsage: Double,
    maxMemUsage: Double,
    avgMemUsage: Double,
    startMemUsage: Double,
    endMemUsage: Double
)

@Path("/statistics")
@Produces(Array(MediaType.APPLICATION_JSON))
class StatisticsResource {

  final private lazy val context = SqlServer.getInstance().createDSLContext()

  @POST
  @Path("/workflow")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def saveWorkflowStats(
      stats: WorkflowStatisticsRequest,
      @Auth sessionUser: SessionUser
  ): Unit = {
    context
      .insertInto(DSL.table("workflow_statistics"))
      .columns(
        DSL.field("workflow_id"),
        DSL.field("execution_id"),
        DSL.field("cpu_usage_max"),
        DSL.field("cpu_usage_avg"),
        DSL.field("cpu_usage_start"),
        DSL.field("cpu_usage_end"),
        DSL.field("mem_usage_max"),
        DSL.field("mem_usage_avg"),
        DSL.field("mem_usage_start"),
        DSL.field("mem_usage_end")
      )
      .values(
        stats.workflow_id,
        stats.execution_id,
        stats.cpu_usage_max,
        stats.cpu_usage_avg,
        stats.cpu_usage_start,
        stats.cpu_usage_end,
        stats.mem_usage_max,
        stats.mem_usage_avg,
        stats.mem_usage_start,
        stats.mem_usage_end
      )
      .onDuplicateKeyIgnore()
      .execute()
  }

  @GET
  @Path("/workflow/{wid}")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def getAggregatedWorkflowStats(
      @PathParam("wid") wid: Int,
      @Auth sessionUser: SessionUser
  ): AggregatedWorkflowStatistics = {
    val records = context
      .select()
      .from(DSL.table("workflow_statistics"))
      .where(DSL.field("workflow_id").eq(wid))
      .fetch()
      .asScala

    if (records.isEmpty) {
      return null
    }

    val cpuMax = records.map(_.get("cpu_usage_max", classOf[Double])).max
    val cpuAvg = records.map(_.get("cpu_usage_avg", classOf[Double])).sum / records.size
    val cpuStart = records.map(_.get("cpu_usage_start", classOf[Double])).sum / records.size
    val cpuEnd = records.map(_.get("cpu_usage_end", classOf[Double])).sum / records.size
    val memMax = records.map(_.get("mem_usage_max", classOf[Double])).max
    val memAvg = records.map(_.get("mem_usage_avg", classOf[Double])).sum / records.size
    val memStart = records.map(_.get("mem_usage_start", classOf[Double])).sum / records.size
    val memEnd = records.map(_.get("mem_usage_end", classOf[Double])).sum / records.size

    AggregatedWorkflowStatistics(
      cpuMax,
      cpuAvg,
      cpuStart,
      cpuEnd,
      memMax,
      memAvg,
      memStart,
      memEnd
    )
  }
}
