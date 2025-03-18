SELECT 1 as cnt FROM read_files( :volume_path || "/customers.csv", format => "csv", header => true, inferSchema => true, mode => "DROPMALFORMED")
GROUP BY ALL
UNION ALL 
SELECT 1 FROM read_files( :volume_path || "/customers.csv", format => "csv", header => true, inferSchema => true, mode => "DROPMALFORMED")
GROUP BY ALL
UNION ALL
SELECT 1 FROM IDENTIFIER(:catalog_name || "." || :target_schema || "." || "raw_events")
GROUP BY ALL
;