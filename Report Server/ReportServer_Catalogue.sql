WITH ReportDatasets AS (
    SELECT
        c.ItemID,
        c.Name AS ReportName,
        c.Path AS ReportPath,

        ds.value('@Name','nvarchar(256)') AS DatasetName,

        ds.value('(*[local-name()="Query"]/*[local-name()="CommandType"]/text())[1]', 'nvarchar(50)') AS ReportCommandType,
        ds.value('(*[local-name()="Query"]/*[local-name()="CommandText"]/text())[1]', 'nvarchar(max)') AS ReportCommandText,

        ds.value('(*[local-name()="SharedDataSet"]/*[local-name()="SharedDataSetReference"]/text())[1]', 'nvarchar(1024)') AS SharedDataSetReference
    FROM ReportServer.dbo.Catalog c
    CROSS APPLY (SELECT TRY_CAST(CAST(c.Content AS varbinary(max)) AS xml) AS ReportXml) r
    CROSS APPLY r.ReportXml.nodes('/*[local-name()="Report"]/*[local-name()="DataSets"]/*[local-name()="DataSet"]') AS X(ds)
    WHERE c.Type = 2  -- Reports
),
SharedDatasetDefs AS (
    SELECT
        c.Name AS SharedDataSetName,
        c.Path AS SharedDataSetPath,

        sds.value('(*[local-name()="Query"]/*[local-name()="CommandType"]/text())[1]', 'nvarchar(50)') AS SharedCommandType,
        sds.value('(*[local-name()="Query"]/*[local-name()="CommandText"]/text())[1]', 'nvarchar(max)') AS SharedCommandText
    FROM ReportServer.dbo.Catalog c
    CROSS APPLY (SELECT TRY_CAST(CAST(c.Content AS varbinary(max)) AS xml) AS SharedXml) r
    CROSS APPLY r.SharedXml.nodes('/*[local-name()="SharedDataSet"]/*[local-name()="DataSet"]') AS X(sds)
    WHERE c.Type = 8  -- Shared Datasets
)
SELECT
    rd.ReportName,
    rd.ReportPath,
    rd.DatasetName,

    CASE
        WHEN rd.SharedDataSetReference IS NOT NULL AND LTRIM(RTRIM(rd.SharedDataSetReference)) <> '' THEN 'SHARED'
        WHEN rd.ReportCommandType IS NOT NULL AND LOWER(rd.ReportCommandType) = 'storedprocedure' THEN 'STORED_PROC'
        WHEN rd.ReportCommandText IS NOT NULL AND LTRIM(RTRIM(rd.ReportCommandText)) <> '' THEN 'INLINE_SQL'
        ELSE 'UNKNOWN'
    END AS DatasetKind,

    -- If shared dataset is used, take its CommandType/CommandText, otherwise report's
    COALESCE(sd.SharedCommandType, rd.ReportCommandType) AS ResolvedCommandType,
    COALESCE(sd.SharedCommandText, rd.ReportCommandText) AS ResolvedCommandText,

    sd.SharedDataSetPath,
    rd.SharedDataSetReference
FROM ReportDatasets rd
LEFT JOIN SharedDatasetDefs sd
    ON sd.SharedDataSetName = rd.SharedDataSetReference
