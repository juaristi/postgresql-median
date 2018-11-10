CREATE OR REPLACE FUNCTION _median_transfn(state internal, val anyelement)
RETURNS internal
AS '/home/a/code/postgresql-10.4/src/backend/timescale-assignment/median.so', 'median_transfn'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION _median_finalfn(state internal, val anyelement)
RETURNS anyelement
AS '/home/a/code/postgresql-10.4/src/backend/timescale-assignment/median.so', 'median_finalfn'
LANGUAGE C IMMUTABLE;

DROP AGGREGATE IF EXISTS median (ANYELEMENT);
CREATE AGGREGATE median (ANYELEMENT)
(
    sfunc = _median_transfn,
    stype = internal,
    finalfunc = _median_finalfn,
    finalfunc_extra
);
