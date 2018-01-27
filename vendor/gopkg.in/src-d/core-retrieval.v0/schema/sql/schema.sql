CREATE TABLE IF NOT EXISTS repositories (
	id uuid PRIMARY KEY,
	created_at timestamptz,
        updated_at timestamptz,
        endpoints text[],
        status varchar(20),
        fetched_at timestamptz,
        fetch_error_at timestamptz,
        last_commit_at timestamptz,
        is_fork boolean
);

CREATE INDEX IF NOT EXISTS idx_repositories_endpoints on "repositories" USING GIN ("endpoints");

CREATE TABLE IF NOT EXISTS repository_references (
        id uuid PRIMARY KEY,
        created_at timestamptz,
        updated_at timestamptz,
        name text,
        repository_id uuid references repositories(id),
        hash text,
        init text,
        roots text[],
        reference_time timestamptz
);

CREATE INDEX IF NOT EXISTS idx_references_repository_id ON "repository_references" ("repository_id");
CREATE INDEX IF NOT EXISTS idx_references_init ON "repository_references" ("init");
