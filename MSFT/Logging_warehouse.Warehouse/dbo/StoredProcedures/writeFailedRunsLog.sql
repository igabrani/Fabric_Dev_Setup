CREATE PROCEDURE writeFailedRunsLog(@PipelineName VARCHAR(8000),@RunStatus VARCHAR(8000),@RunDate datetime2(6))
AS
INSERT pipelinesFailedRuns(PipelineName,RunStatus,RunDate)
VALUES(@PipelineName,@RunStatus,@RunDate)