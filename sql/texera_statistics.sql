SET search_path TO texera_db, public;

CREATE TABLE IF NOT EXISTS workflow_statistics
(
    workflow_id        INT NOT NULL,
    execution_id       INT NOT NULL,
    cpu_usage_max      DOUBLE PRECISION,
    cpu_usage_avg      DOUBLE PRECISION,
    cpu_usage_start    DOUBLE PRECISION,
    cpu_usage_end      DOUBLE PRECISION,
    mem_usage_max      DOUBLE PRECISION,
    mem_usage_avg      DOUBLE PRECISION,
    mem_usage_start    DOUBLE PRECISION,
    mem_usage_end      DOUBLE PRECISION,
    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (workflow_id, execution_id),
    FOREIGN KEY (workflow_id) REFERENCES workflow(wid) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS operator_statistics
(
    workflow_id        INT NOT NULL,
    execution_id       INT NOT NULL,
    operator_id        VARCHAR(100) NOT NULL,
    cpu_usage_max      DOUBLE PRECISION,
    cpu_usage_avg      DOUBLE PRECISION,
    cpu_usage_start    DOUBLE PRECISION,
    cpu_usage_end      DOUBLE PRECISION,
    mem_usage_max      DOUBLE PRECISION,
    mem_usage_avg      DOUBLE PRECISION,
    mem_usage_start    DOUBLE PRECISION,
    mem_usage_end      DOUBLE PRECISION,
    created_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (workflow_id, execution_id, operator_id),
    FOREIGN KEY (workflow_id) REFERENCES workflow(wid) ON DELETE CASCADE
);
