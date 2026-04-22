# JPointDB vs DuckDB — ClickBench side-by-side

- Generated: 2026-04-22 16:16:48 MSK
- Host: `Darwin arm64`
- JDK: `openjdk version 25.0.1 2025-10-21 LTS`
- Rows: 1000000
- Runs per query: 3 (first discarded as warm-up; reported = min of remaining 2)
- JPointDB time = server-side `elapsedMs`; DuckDB time = `.timer` real

| # | JPointDB, ms | DuckDB, ms | ratio (jp/duck) | query |
|---|-----:|-----:|-----:|:------|
| 01 | 0.058 | 0.200 | 0.29 | `SELECT COUNT(*) FROM hits;` |
| 02 | 1.792 | 2.200 | 0.81 | `SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;` |
| 03 | 0.164 | 0.600 | 0.27 | `SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;` |
| 04 | 0.227 | 2.200 | 0.10 | `SELECT AVG(UserID) FROM hits;` |
| 05 | 5.956 | 2.200 | 2.71 | `SELECT COUNT(DISTINCT UserID) FROM hits;` |
| 06 | 2.754 | 4.000 | 0.69 | `SELECT COUNT(DISTINCT SearchPhrase) FROM hits;` |
| 07 | 0.012 | 0.200 | 0.06 | `SELECT MIN(EventDate), MAX(EventDate) FROM hits;` |
| 08 | 1.858 | 5.400 | 0.34 | `SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDE...` |
| 09 | 9.583 | 0.600 | 15.97 | `SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DES...` |
| 10 | 14.554 | 4.000 | 3.64 | `SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT ...` |
| 11 | 1.434 | 2.600 | 0.55 | `SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <...` |
| 12 | 1.477 | 3.400 | 0.43 | `SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE Mobil...` |
| 13 | 2.369 | 3.000 | 0.79 | `SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPh...` |
| 14 | 3.936 | 1.800 | 2.19 | `SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GRO...` |
| 15 | 3.044 | 0.200 | 15.22 | `SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' G...` |
| 16 | 5.003 | 0.600 | 8.34 | `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;` |
| 17 | 6.627 | 0.200 | 33.13 | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY ...` |
| 18 | 7.090 | 1.800 | 3.94 | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY ...` |
| 19 | 75.737 | 2.200 | 34.43 | `SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GR...` |
| 20 | 2.961 | 2.600 | 1.14 | `SELECT UserID FROM hits WHERE UserID = 435090932899640449;` |
| 21 | 7.519 | 0.200 | 37.59 | `SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';` |
| 22 | 9.773 | 5.000 | 1.95 | `SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND Se...` |
| 23 | 7.721 | 1.000 | 7.72 | `SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM h...` |
| 24 | 7.847 | 6.200 | 1.27 | `SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;` |
| 25 | 1.601 | 2.200 | 0.73 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;` |
| 26 | 1.525 | 12.000 | 0.13 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;` |
| 27 | 1.655 | 3.000 | 0.55 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase...` |
| 28 | 8.008 | 1.800 | 4.45 | `SELECT CounterID, AVG(STRLEN(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP ...` |
| 29 | 42.332 | 0.000 | — | `SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(STRLE...` |
| 30 | 0.269 | 0.400 | 0.67 | `SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(Re...` |
| 31 | 4.436 | 0.200 | 22.18 | `SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FR...` |
| 32 | 9.088 | 3.200 | 2.84 | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits...` |
| 33 | 39.148 | 2.000 | 19.57 | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits...` |
| 34 | 7.643 | 2.200 | 3.47 | `SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;` |
| 35 | 5.962 | 4.000 | 1.49 | `SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;` |
| 36 | 25.200 | 5.800 | 4.34 | `SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROU...` |
| 37 | 15.521 | 1.400 | 11.09 | `SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013...` |
| 38 | 9.481 | 1.200 | 7.90 | `SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '20...` |
| 39 | 8.000 | 2.400 | 3.33 | `SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013...` |
| 40 | 53.002 | 10.200 | 5.20 | `SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND A...` |
| 41 | 10.401 | 3.600 | 2.89 | `SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND Eve...` |
| 42 | 10.413 | 3.600 | 2.89 | `SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE Cou...` |
| 43 | 13.853 | 2.000 | 6.93 | `SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE Coun...` |
| — | **447.034** | **113.600** | **3.94** | _sum_ |

Lower is better. `ratio > 1` means JPointDB is slower than DuckDB on that query.
