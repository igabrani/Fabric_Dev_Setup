CREATE PROCEDURE writeLog(@logData VARCHAR(8000))
AS
INSERT pipelineLogs(EventDate,LogData)
VALUES(GetDate(),@logData)