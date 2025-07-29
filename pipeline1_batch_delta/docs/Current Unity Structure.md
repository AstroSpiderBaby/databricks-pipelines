


[Blob Storage / Uploads]  --->  /Volumes/thebetty/bronze/landing_zone/*.csv
                                      |
                                      v
                            Ingest scripts read CSV and write to:
                                      |
                           /Volumes/thebetty/bronze/{target_volume}
                                      |
                                      v
                            Optionally registered as UC tables:
                         thebetty.bronze.{finances_invoices, etc.}
Meanwhile, SQL Server bypasses landing zone and goes:

rust
Copy
Edit
[JDBC] ---> DataFrame ---> Delta write ---> /Volumes/thebetty/bronze/vendor_compliance
                                                    |
                                                    v
                                     Unity Catalog Table registered
                                 as thebetty.bronze.vendor_compliance
✅ That’s a valid pattern. Unity Catalog supports having either Volumes (file-level) or Tables (registered metadata).