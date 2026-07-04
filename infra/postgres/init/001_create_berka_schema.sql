-- =============================================================================
-- 001_create_berka_schema.sql
-- Chạy tự động khi Postgres container khởi động lần đầu
-- (mount vào /docker-entrypoint-initdb.d/)
--
-- Làm 3 việc theo thứ tự:
--   1. Tạo database 'berka' riêng (tách với database 'airflow' của Airflow)
--   2. Tạo user 'replicator' cho Debezium đọc WAL
--   3. Tạo 5 bảng Berka trong database 'berka'
--
-- LƯU Ý QUAN TRỌNG VỀ SCRIPT NÀY:
-- Postgres chạy init scripts với user POSTGRES_USER (airflow) trong context
-- của database POSTGRES_DB (airflow). Để tạo objects trong database 'berka',
-- phải dùng \connect hoặc chạy trong transaction riêng.
-- =============================================================================


-- =============================================================================
-- PHẦN 1: Tạo database berka và user replicator
-- Chạy trong context database 'airflow' (mặc định khi init)
-- =============================================================================

-- Tạo database berka riêng cho Berka banking data
-- Tách hoàn toàn với database airflow của Airflow metadata
CREATE DATABASE berka
    WITH
    OWNER = airflow
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8'
    LC_CTYPE = 'en_US.utf8'
    TEMPLATE = template0;

COMMENT ON DATABASE berka IS 'Berka PKDD99 banking dataset — core banking simulation';

-- Tạo user replicator riêng cho Debezium
-- LÝ DO tách riêng thay vì dùng user airflow:
--   - Principle of least privilege: Debezium chỉ cần quyền đọc WAL,
--     không nên có quyền write vào Airflow metadata
--   - Dễ revoke/rotate credential Debezium mà không ảnh hưởng Airflow
--   - Đúng pattern production: mỗi service có service account riêng
CREATE USER replicator WITH
    REPLICATION      -- quyền tạo replication slot, đọc WAL
    LOGIN
    PASSWORD 'replicator_password';  -- đổi trong .env khi deploy thật

-- Grant quyền connect vào berka database
GRANT CONNECT ON DATABASE berka TO replicator;


-- =============================================================================
-- PHẦN 2: Tạo schema và 5 bảng Berka trong database 'berka'
-- Phải switch sang database berka trước khi tạo tables
-- =============================================================================

\connect berka


-- Grant replicator quyền đọc tất cả tables trong schema public
-- cần thiết để Debezium đọc initial snapshot + ongoing changes
GRANT USAGE ON SCHEMA public TO replicator;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replicator;

-- Đảm bảo tables tạo sau này cũng tự động grant SELECT cho replicator
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO replicator;


-- ---------------------------------------------------------------------------
-- Bảng 1: district
-- Thông tin demographic khu vực — bảng tham chiếu, ít thay đổi nhất
-- Tạo trước vì account, client có FK sang district
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS district (
    district_id         INTEGER         PRIMARY KEY,
    name                VARCHAR(100)    NOT NULL,
    region              VARCHAR(100),
    num_inhabitants     INTEGER,
    num_municipalities_lt_499    INTEGER,
    num_municipalities_500_1999  INTEGER,
    num_municipalities_2000_9999 INTEGER,
    num_municipalities_gt_10000  INTEGER,
    num_cities          INTEGER,
    ratio_urban         NUMERIC(5, 2),
    avg_salary          NUMERIC(10, 2),
    unemployment_rate_95 NUMERIC(5, 2),
    unemployment_rate_96 NUMERIC(5, 2),
    num_entrepreneurs_per_1000   NUMERIC(7, 2),
    num_crimes_95       INTEGER,
    num_crimes_96       INTEGER
);

COMMENT ON TABLE district IS
    'Demographic info của khu vực — tham chiếu bởi account và client';


-- ---------------------------------------------------------------------------
-- Bảng 2: account
-- Thông tin tài khoản ngân hàng — bảng tham chiếu cho trans
-- Tạo trước trans và disposition vì cả 2 có FK sang account
--
-- LƯU Ý cột date:
-- Berka dùng format YYMMDD nén thành INTEGER (vd: 930101 = 1993-01-01)
-- Lưu INTEGER ở đây, dbt staging sẽ cast sang DATE:
--   TO_DATE(CAST(date AS TEXT), 'YYMMDD')
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS account (
    account_id          INTEGER         PRIMARY KEY,
    district_id         INTEGER         NOT NULL
                            REFERENCES district(district_id),
    frequency           VARCHAR(50)     NOT NULL,
    -- YYMMDD format, ví dụ: 930101 = 1993-01-01
    -- Cast sang DATE ở dbt staging layer, không phải ở đây
    date                INTEGER         NOT NULL
);

COMMENT ON TABLE account IS
    'Tài khoản ngân hàng — tham chiếu bởi trans và disposition';
COMMENT ON COLUMN account.date IS
    'Ngày mở tài khoản, format YYMMDD (INTEGER). Cast sang DATE ở dbt staging.';


-- ---------------------------------------------------------------------------
-- Bảng 3: client
-- Thông tin cá nhân khách hàng
-- birth_number: format YYMMDD (nam) hoặc YYMM+50DD (nữ)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS client (
    client_id           INTEGER         PRIMARY KEY,
    -- birth_number encode cả ngày sinh lẫn giới tính:
    --   Nam:  YYMMDD
    --   Nữ:   YY(MM+50)DD  — tháng cộng thêm 50
    -- Ví dụ: 706213 = sinh 1970-12-13, nam
    --        755502 = sinh 1975-05-02, nữ (55 = 05 + 50)
    -- Decode logic nằm ở dbt staging layer
    birth_number        INTEGER         NOT NULL,
    district_id         INTEGER         NOT NULL
                            REFERENCES district(district_id)
);

COMMENT ON TABLE client IS 'Thông tin cá nhân khách hàng';
COMMENT ON COLUMN client.birth_number IS
    'Encode ngày sinh + giới tính: YYMMDD (nam) hoặc YY(MM+50)DD (nữ). Decode ở dbt staging.';


-- ---------------------------------------------------------------------------
-- Bảng 4: disposition
-- Liên kết nhiều-nhiều giữa client và account
-- 1 account có thể có nhiều client (owner + disponent)
-- 1 client có thể có nhiều account
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS disposition (
    disp_id             INTEGER         PRIMARY KEY,
    client_id           INTEGER         NOT NULL
                            REFERENCES client(client_id),
    account_id          INTEGER         NOT NULL
                            REFERENCES account(account_id),
    -- 'OWNER': chủ tài khoản — có quyền đầy đủ
    -- 'DISPONENT': người được ủy quyền — chỉ có thể rút tiền
    type                VARCHAR(20)     NOT NULL
                            CHECK (type IN ('OWNER', 'DISPONENT'))
);

COMMENT ON TABLE disposition IS
    'Liên kết N-N giữa client và account. Dùng để lookup client_id từ account_id trong Speed Layer.';


-- ---------------------------------------------------------------------------
-- Bảng 5: trans
-- Giao dịch ngân hàng — bảng chính, volume cao nhất
-- Đây là bảng Debezium track CDC liên tục (cdc.public.trans topic)
--
-- Các cột nullable:
--   operation  — NULL khi type=VYBER (legacy withdrawal code)
--   k_symbol   — NULL/empty khi không có purpose code
--   bank       — NULL khi không phải chuyển khoản liên ngân hàng
--   account    — NULL khi không phải chuyển khoản liên ngân hàng
--
-- account_id KHÔNG có FK constraint sang bảng account ở đây —
-- vì data generator sẽ tiêm referential violation (account_id giả)
-- như 1 trong 3 loại lỗi có chủ đích. FK constraint sẽ reject insert đó.
-- Referential integrity được kiểm tra ở dbt test và Speed Layer DLQ,
-- không phải ở DB constraint.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trans (
    trans_id            INTEGER         PRIMARY KEY,
    account_id          INTEGER         NOT NULL,
    -- YYMMDD format — xem comment ở bảng account.date
    date                INTEGER         NOT NULL,
    -- 'PRIJEM' = credit (thu vào)
    -- 'VYDAJ'  = debit  (chi ra)
    -- 'VYBER'  = withdrawal (rút tiền mặt — legacy code, ít dùng)
    type                VARCHAR(10)     NOT NULL
                            CHECK (type IN ('PRIJEM', 'VYDAJ', 'VYBER')),
    -- NULL khi type = 'VYBER' (legacy)
    -- 'VKLAD'         = gửi tiền mặt
    -- 'PREVOD Z UCTU' = nhận chuyển khoản
    -- 'PREVOD NA UCET'= chuyển khoản đi
    -- 'VYBER KARTOU'  = rút tiền qua thẻ
    -- 'VYBER'         = rút tiền mặt (không qua thẻ)
    operation           VARCHAR(30),
    amount              NUMERIC(12, 2)  NOT NULL
                            CHECK (amount >= 0),
    balance             NUMERIC(12, 2),
    -- Purpose code — nhiều giá trị NULL hoặc empty string trong data gốc
    -- empty string ('') là category hợp lệ riêng, KHÁC với NULL
    k_symbol            VARCHAR(20),
    -- Chỉ có giá trị khi là chuyển khoản liên ngân hàng
    bank                VARCHAR(10),
    account             VARCHAR(20)
);

COMMENT ON TABLE trans IS
    'Giao dịch ngân hàng — volume cao, CDC tracked qua Debezium → cdc.public.trans';
COMMENT ON COLUMN trans.account_id IS
    'KHÔNG có FK constraint — data generator tiêm referential violation cố ý. '
    'Kiểm tra integrity ở dbt test relationships() và Speed Layer DLQ.';
COMMENT ON COLUMN trans.date IS
    'Ngày giao dịch, format YYMMDD (INTEGER). Cast sang DATE ở dbt staging.';
COMMENT ON COLUMN trans.k_symbol IS
    'Purpose code. Empty string và NULL là 2 giá trị khác nhau trong data gốc Berka.';


-- =============================================================================
-- PHẦN 3: Index cho performance
-- Chỉ tạo index trên các cột thường xuyên dùng trong JOIN/filter
-- =============================================================================

-- trans.account_id — dùng nhiều nhất: Speed Layer lookup, dbt join, Debezium filter
CREATE INDEX idx_trans_account_id ON trans(account_id);

-- trans.date — dùng trong dbt incremental filter và time-range query
CREATE INDEX idx_trans_date ON trans(date);

-- disposition — Speed Layer cần lookup theo account_id để tìm client
CREATE INDEX idx_disposition_account_id ON disposition(account_id);
CREATE INDEX idx_disposition_client_id ON disposition(client_id);


-- =============================================================================
-- PHẦN 4: Verify — in ra summary để xác nhận script chạy thành công
-- Sẽ thấy output này trong docker compose logs postgres lần đầu up
-- =============================================================================
DO $$
BEGIN
    RAISE NOTICE '=== Berka schema khởi tạo thành công ===';
    RAISE NOTICE 'Database: berka';
    RAISE NOTICE 'Tables: district, account, client, disposition, trans';
    RAISE NOTICE 'User replicator: tạo thành công (REPLICATION privilege)';
    RAISE NOTICE 'Indexes: trans(account_id, date), disposition(account_id, client_id)';
    RAISE NOTICE 'Sẵn sàng cho Debezium CDC và data loading.';
END $$;