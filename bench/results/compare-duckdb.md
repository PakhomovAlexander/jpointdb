# JPointDB vs DuckDB — ClickBench side-by-side

- Generated: 2026-04-23 16:17:48 MSK
- Host: `Darwin arm64`
- JDK: `openjdk version 25.0.1 2025-10-21 LTS`
- Rows: 1000000
- Runs per query: 4 (first discarded as warm-up; reported = min of remaining 3)
- JPointDB time = server-side `elapsedMs`; DuckDB time = `.timer` real

| # | JPointDB, ms | DuckDB, ms | ratio (jp/duck) | query |
|---|-----:|-----:|-----:|:------|
| 01 | 0.249 | 0.500 | 0.50 | `SELECT COUNT(*) FROM hits;` |
| 02 | 0.437 | 0.200 | 2.19 | `SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;` |
| 03 | 0.295 | 2.100 | 0.14 | `SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;` |
| 04 | 0.274 | 2.500 | 0.11 | `SELECT AVG(UserID) FROM hits;` |
| 05 | 6.014 | 0.200 | 30.07 | `SELECT COUNT(DISTINCT UserID) FROM hits;` |
| 06 | 2.501 | 5.700 | 0.44 | `SELECT COUNT(DISTINCT SearchPhrase) FROM hits;` |
| 07 | 0.055 | 1.000 | 0.06 | `SELECT MIN(EventDate), MAX(EventDate) FROM hits;` |
| 08 | 0.622 | 2.700 | 0.23 | `SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDE...` |
| 09 | 9.125 | 3.700 | 2.47 | `SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DES...` |
| 10 | 13.761 | 3.000 | 4.59 | `SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT ...` |
| 11 | 1.513 | 0.100 | 15.13 | `SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <...` |
| 12 | 1.420 | 0.500 | 2.84 | `SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE Mobil...` |
| 13 | 2.114 | 0.300 | 7.05 | `SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPh...` |
| 14 | 3.972 | 1.900 | 2.09 | `SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GRO...` |
| 15 | 2.301 | 2.400 | 0.96 | `SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' G...` |
| 16 | 3.946 | 0.300 | 13.15 | `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;` |
| 17 | 5.175 | 1.400 | 3.70 | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY ...` |
| 18 | 5.325 | 1.200 | 4.44 | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY ...` |
| 19 | 12.446 | 2.400 | 5.19 | `SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GR...` |
| 20 | 0.129 | 3.700 | 0.03 | `SELECT UserID FROM hits WHERE UserID = 435090932899640449;` |
| 21 | 0.861 | 1.900 | 0.45 | `SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';` |
| 22 | 1.130 | 0.100 | 11.30 | `SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND Se...` |
| 23 | 1.432 | 0.500 | 2.86 | `SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM h...` |
| 24 | 1.273 | 0.300 | 4.24 | `SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;` |
| 25 | 1.876 | 2.000 | 0.94 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;` |
| 26 | 1.741 | 2.600 | 0.67 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;` |
| 27 | 1.915 | 0.300 | 6.38 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase...` |
| 28 | 6.603 | 1.000 | 6.60 | `SELECT CounterID, AVG(STRLEN(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP ...` |
| 29 | 10.302 | 4.200 | 2.45 | `SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(STRLE...` |
| 30 | 0.321 | 2.700 | 0.12 | `SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(Re...` |
| 31 | 4.102 | 2.900 | 1.41 | `SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FR...` |
| 32 | 8.994 | 1.800 | 5.00 | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits...` |
| 33 | 42.946 | 0.200 | 214.73 | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits...` |
| 34 | 6.277 | 0.300 | 20.92 | `SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;` |
| 35 | 5.669 | 1.900 | 2.98 | `SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;` |
| 36 | 4.578 | 2.000 | 2.29 | `SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROU...` |
| 37 | 13.278 | 4.000 | 3.32 | `SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013...` |
| 38 | 8.810 | 5.100 | 1.73 | `SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '20...` |
| 39 | 2.394 | 0.900 | 2.66 | `SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013...` |
| 40 | 16.198 | 2.400 | 6.75 | `SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND A...` |
| 41 | 11.260 | 10.800 | 1.04 | `SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND Eve...` |
| 42 | 3.376 | 2.800 | 1.21 | `SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE Cou...` |
| 43 | 2.870 | 1.900 | 1.51 | `SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE Coun...` |
| — | **229.880** | **88.400** | **2.60** | _sum_ |

Lower is better. `ratio > 1` means JPointDB is slower than DuckDB on that query.
