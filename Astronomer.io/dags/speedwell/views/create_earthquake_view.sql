CREATE VIEW IF NOT EXISTS
 `{project}.{dataset}.v_earthquake`
 as
SELECT
time,
EXTRACT(YEAR FROM cast(time as TIMESTAMP)) as YEAR,
latitude,
longitude,
CAST(depth as NUMERIC) depth,
CAST(mag as NUMERIC) magnitude,
magType,
nst,
gap,
dmin,
rms,
net,
id,
updated,
place,
type,
horizontalError,
depthError,
magError,
magNst,
status,
locationSource,
magSource,
Territory
 from
`{project}.{dataset}.t_earthquake`