# JPointDB vs DuckDB — ClickBench side-by-side

- Generated: 2026-04-20 19:20:30 MSK
- Host: `Darwin arm64`
- JDK: `openjdk version 25.0.1 2025-10-21 LTS`
- Rows: 1000000
- Runs per query: 3 (first discarded as warm-up; reported = min of remaining 2)
- JPointDB time = server-side `elapsedMs`; DuckDB time = `.timer` real

| # | JPointDB, ms | DuckDB, ms | ratio (jp/duck) | query |
|---|-----:|-----:|-----:|:------|
| 01 | 5.707 | 0.600 | 9.51 | `SELECT COUNT(*) FROM hits;` |
| 02 | 9.971 | 2.200 | 4.53 | `SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;` |
| 03 | 20.497 | 0.400 | 51.24 | `SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;` |
| 04 | 14.711 | 2.200 | 6.69 | `SELECT AVG(UserID) FROM hits;` |
| 05 | 23.132 | 2.400 | 9.64 | `SELECT COUNT(DISTINCT UserID) FROM hits;` |
| 06 | 16.593 | 4.000 | 4.15 | `SELECT COUNT(DISTINCT SearchPhrase) FROM hits;` |
| 07 | 22.501 | 0.200 | 112.50 | `SELECT MIN(EventDate), MAX(EventDate) FROM hits;` |
| 08 | 10.149 | 6.600 | 1.54 | `SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDE...` |
| 09 | 44.600 | 1.000 | 44.60 | `SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DES...` |
| 10 | 68.219 | 3.800 | 17.95 | `SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT ...` |
| 11 | 13.419 | 2.800 | 4.79 | `SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <...` |
| 12 | 13.588 | 3.400 | 4.00 | `SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE Mobil...` |
| 13 | 16.636 | 2.800 | 5.94 | `SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPh...` |
| 14 | 19.648 | 2.200 | 8.93 | `SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GRO...` |
| 15 | 17.756 | 0.200 | 88.78 | `SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' G...` |
| 16 | 40.267 | 0.600 | 67.11 | `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;` |
| 17 | 56.682 | 0.200 | 283.41 | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY ...` |
| 18 | 54.396 | 1.600 | 34.00 | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY ...` |
| 19 | 225.523 | 2.000 | 112.76 | `SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GR...` |
| 20 | 13.161 | 2.400 | 5.48 | `SELECT UserID FROM hits WHERE UserID = 435090932899640449;` |
| 21 | 64.016 | 0.200 | 320.08 | `SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';` |
| 22 | 67.980 | 4.800 | 14.16 | `SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND Se...` |
| 23 | 58.273 | 0.800 | 72.84 | `SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM h...` |
| 24 | 64.557 | 6.000 | 10.76 | `SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;` |
| 25 | 33.527 | 2.200 | 15.24 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;` |
| 26 | 32.047 | 11.400 | 2.81 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;` |
| 27 | 34.605 | 3.000 | 11.53 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase...` |
| 28 | 75.603 | 1.800 | 42.00 | `SELECT CounterID, AVG(STRLEN(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP ...` |
| 29 | 358.045 | 0.200 | 1790.22 | `SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(STRLE...` |
| 30 | 1324.165 | 0.400 | 3310.41 | `SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(Re...` |
| 31 | 27.345 | 0.200 | 136.72 | `SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FR...` |
| 32 | 49.097 | 3.200 | 15.34 | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits...` |
| 33 | 1125.660 | 2.000 | 562.83 | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits...` |
| 34 | 116.578 | 2.200 | 52.99 | `SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;` |
| 35 | 129.461 | 3.800 | 34.07 | `SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;` |
| 36 | 137.961 | 5.800 | 23.79 | `SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROU...` |
| 37 | 118.962 | 1.400 | 84.97 | `SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013...` |
| 38 | 69.652 | 1.000 | 69.65 | `SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '20...` |
| 39 | 55.211 | 2.400 | 23.00 | `SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013...` |
| 40 | 181.032 | 10.200 | 17.75 | `SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND A...` |
| 41 | 78.959 | 3.600 | 21.93 | `SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND Eve...` |
| 42 | 72.726 | 3.600 | 20.20 | `SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE Cou...` |
| 43 | 91.550 | 2.000 | 45.77 | `SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE Coun...` |
| — | **5074.168** | **113.800** | **44.59** | _sum_ |

Lower is better. `ratio > 1` means JPointDB is slower than DuckDB on that query.
