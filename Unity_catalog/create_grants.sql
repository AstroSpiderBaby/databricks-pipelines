-- Grant access to entire catalog
GRANT USAGE ON CATALOG thebetty TO `account users`;

-- Grant access to individual schemas
GRANT USAGE ON SCHEMA thebetty.bronze TO `account users`;
GRANT USAGE ON SCHEMA thebetty.silver TO `account users`;
GRANT USAGE ON SCHEMA thebetty.gold TO `account users`;

-- Optionally: allow table creation
GRANT CREATE ON SCHEMA thebetty.bronze TO `account users`;
GRANT CREATE ON SCHEMA thebetty.silver TO `account users`;
GRANT CREATE ON SCHEMA thebetty.gold TO `account users`;
