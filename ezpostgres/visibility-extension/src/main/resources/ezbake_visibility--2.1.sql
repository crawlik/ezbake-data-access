CREATE OR REPLACE FUNCTION should_purge(BIGINT[], VARCHAR) RETURNS BOOLEAN
     AS 'ezbake_visibility', 'should_purge' LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION is_composite(VARCHAR) RETURNS BOOLEAN
     AS 'ezbake_visibility', 'is_composite' LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION get_purge_id(VARCHAR) RETURNS BIGINT
     AS 'ezbake_visibility', 'get_purge_id' LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION verify_row_visible(VARCHAR, VARCHAR) RETURNS BOOLEAN
     AS 'ezbake_visibility', 'verify_row_visible' LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION verify_row_visible_current_setting(VARCHAR) RETURNS BOOLEAN
     AS 'ezbake_visibility', 'verify_row_visible_current_setting' LANGUAGE C STRICT;
