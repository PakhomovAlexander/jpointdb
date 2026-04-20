# JPointDB vs DuckDB — ClickBench side-by-side

- Generated: 2026-04-20 18:00:50 MSK
- Host: `Darwin arm64`
- JDK: `openjdk version 25.0.1 2025-10-21 LTS`
- Rows: 1000000
- Runs per query: 3 (first discarded as warm-up; reported = min of remaining 2)
- JPointDB time = server-side `elapsedMs`; DuckDB time = `.timer` real

| # | JPointDB, ms | DuckDB, ms | ratio (jp/duck) | query |
|---|-----:|-----:|-----:|:------|
| 01 | 6.890 | 0.600 | 11.48 | `SELECT COUNT(*) FROM hits;` |
| 02 | 13.666 | 2.400 | 5.69 | `SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;` |
| 03 | 23.404 | 0.600 | 39.01 | `SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;` |
| 04 | 12.377 | 2.600 | 4.76 | `SELECT AVG(UserID) FROM hits;` |
| 05 | 22.760 | 2.800 | 8.13 | `SELECT COUNT(DISTINCT UserID) FROM hits;` |
| 06 | 37.560 | 4.400 | 8.54 | `SELECT COUNT(DISTINCT SearchPhrase) FROM hits;` |
| 07 | 74.039 | 0.200 | 370.19 | `SELECT MIN(EventDate), MAX(EventDate) FROM hits;` |
| 08 | 13.933 | 7.600 | 1.83 | `SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDE...` |
| 09 | 56.230 | 1.000 | 56.23 | `SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DES...` |
| 10 | 79.158 | 5.000 | 15.83 | `SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT ...` |
| 11 | 27.507 | 3.200 | 8.60 | `SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <...` |
| 12 | 27.394 | 5.000 | 5.48 | `SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE Mobil...` |
| 13 | 46.876 | 3.200 | 14.65 | `SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPh...` |
| 14 | 51.273 | 2.400 | 21.36 | `SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GRO...` |
| 15 | 48.243 | 0.200 | 241.22 | `SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' G...` |
| 16 | 51.268 | 0.600 | 85.45 | `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;` |
| 17 | 110.639 | 0.400 | 276.60 | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY ...` |
| 18 | 102.063 | 2.400 | 42.53 | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY ...` |
| 19 | 404.907 | 3.200 | 126.53 | `SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GR...` |
| 20 | 14.249 | 3.600 | 3.96 | `SELECT UserID FROM hits WHERE UserID = 435090932899640449;` |
| 21 | 592.722 | 0.200 | 2963.61 | `SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';` |
| 22 | 632.930 | 6.000 | 105.49 | `SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND Se...` |
| 23 | 1585.962 | 1.000 | 1585.96 | `SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM h...` |
| 24 | 590.112 | 6.400 | 92.20 | `SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;` |
| 25 | 80.173 | 2.800 | 28.63 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;` |
| 26 | 83.081 | 13.400 | 6.20 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;` |
| 27 | 89.602 | 3.400 | 26.35 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase...` |
| 28 | 178.387 | 2.400 | 74.33 | `SELECT CounterID, AVG(STRLEN(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP ...` |
| 29 | 743.736 | 0.200 | 3718.68 | `SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(STRLE...` |
| 30 | 1521.519 | 0.600 | 2535.87 | `SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(Re...` |
| 31 | 54.605 | 0.400 | 136.51 | `SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FR...` |
| 32 | 95.165 | 3.800 | 25.04 | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits...` |
| 33 | 1640.756 | 2.200 | 745.80 | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits...` |
| 34 | 256.300 | 2.800 | 91.54 | `SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;` |
| 35 | 287.960 | 4.600 | 62.60 | `SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;` |
| 36 | 157.093 | 6.600 | 23.80 | `SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROU...` |
| 37 | 363.766 | 1.600 | 227.35 | `SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013...` |
| 38 | 433.759 | 1.200 | 361.47 | `SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '20...` |
| 39 | 178.312 | 2.800 | 63.68 | `SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013...` |
| 40 | 383.715 | 12.200 | 31.45 | `SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND A...` |
| 41 | 229.977 | 4.400 | 52.27 | `SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND Eve...` |
| 42 | 198.646 | 4.400 | 45.15 | `SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE Cou...` |
| 43 | 205.458 | 2.200 | 93.39 | `SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE Coun...` |
| — | **11808.172** | **137.000** | **86.19** | _sum_ |

Lower is better. `ratio > 1` means JPointDB is slower than DuckDB on that query.
