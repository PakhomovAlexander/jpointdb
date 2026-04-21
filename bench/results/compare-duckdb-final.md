# JPointDB vs DuckDB — ClickBench side-by-side

- Generated: 2026-04-21 14:09:27 MSK
- Host: `Darwin arm64`
- JDK: `openjdk version 25.0.1 2025-10-21 LTS`
- Rows: 1000000
- Runs per query: 3 (first discarded as warm-up; reported = min of remaining 2)
- JPointDB time = server-side `elapsedMs`; DuckDB time = `.timer` real

| # | JPointDB, ms | DuckDB, ms | ratio (jp/duck) | query |
|---|-----:|-----:|-----:|:------|
| 01 | 0.924 | 0.200 | 4.62 | `SELECT COUNT(*) FROM hits;` |
| 02 | 2.049 | 2.200 | 0.93 | `SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;` |
| 03 | 3.751 | 0.400 | 9.38 | `SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;` |
| 04 | 1.961 | 2.000 | 0.98 | `SELECT AVG(UserID) FROM hits;` |
| 05 | 6.034 | 2.600 | 2.32 | `SELECT COUNT(DISTINCT UserID) FROM hits;` |
| 06 | 2.737 | 4.000 | 0.68 | `SELECT COUNT(DISTINCT SearchPhrase) FROM hits;` |
| 07 | 3.135 | 0.200 | 15.67 | `SELECT MIN(EventDate), MAX(EventDate) FROM hits;` |
| 08 | 2.171 | 5.200 | 0.42 | `SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDE...` |
| 09 | 10.200 | 1.000 | 10.20 | `SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DES...` |
| 10 | 15.577 | 4.400 | 3.54 | `SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT ...` |
| 11 | 2.666 | 2.600 | 1.03 | `SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <...` |
| 12 | 3.030 | 3.600 | 0.84 | `SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE Mobil...` |
| 13 | 3.810 | 2.800 | 1.36 | `SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPh...` |
| 14 | 6.197 | 2.000 | 3.10 | `SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GRO...` |
| 15 | 4.522 | 0.200 | 22.61 | `SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' G...` |
| 16 | 6.678 | 0.400 | 16.70 | `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;` |
| 17 | 11.322 | 0.200 | 56.61 | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY ...` |
| 18 | 10.831 | 2.000 | 5.42 | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY ...` |
| 19 | 85.766 | 2.000 | 42.88 | `SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GR...` |
| 20 | 2.215 | 2.400 | 0.92 | `SELECT UserID FROM hits WHERE UserID = 435090932899640449;` |
| 21 | 7.800 | 0.200 | 39.00 | `SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';` |
| 22 | 8.648 | 5.000 | 1.73 | `SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND Se...` |
| 23 | 7.531 | 1.000 | 7.53 | `SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM h...` |
| 24 | 7.908 | 6.400 | 1.24 | `SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;` |
| 25 | 22.460 | 2.400 | 9.36 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;` |
| 26 | 20.968 | 11.800 | 1.78 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;` |
| 27 | 25.452 | 2.800 | 9.09 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase...` |
| 28 | 6.714 | 1.800 | 3.73 | `SELECT CounterID, AVG(STRLEN(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP ...` |
| 29 | 41.936 | 0.200 | 209.68 | `SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(STRLE...` |
| 30 | 0.318 | 0.600 | 0.53 | `SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(Re...` |
| 31 | 5.359 | 0.200 | 26.79 | `SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FR...` |
| 32 | 10.752 | 3.200 | 3.36 | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits...` |
| 33 | 189.506 | 2.000 | 94.75 | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits...` |
| 34 | 20.145 | 2.400 | 8.39 | `SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;` |
| 35 | 52.387 | 4.000 | 13.10 | `SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;` |
| 36 | 30.013 | 5.800 | 5.17 | `SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROU...` |
| 37 | 23.378 | 1.400 | 16.70 | `SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013...` |
| 38 | 13.400 | 1.000 | 13.40 | `SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '20...` |
| 39 | 10.857 | 2.200 | 4.93 | `SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013...` |
| 40 | 63.314 | 10.400 | 6.09 | `SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND A...` |
| 41 | 14.420 | 3.600 | 4.01 | `SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND Eve...` |
| 42 | 13.248 | 3.600 | 3.68 | `SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE Cou...` |
| 43 | 16.517 | 2.000 | 8.26 | `SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE Coun...` |
| — | **798.607** | **114.400** | **6.98** | _sum_ |

Lower is better. `ratio > 1` means JPointDB is slower than DuckDB on that query.
