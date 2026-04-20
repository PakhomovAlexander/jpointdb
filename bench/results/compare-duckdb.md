# JPointDB vs DuckDB — ClickBench side-by-side

- Generated: 2026-04-20 23:32:03 MSK
- Host: `Darwin arm64`
- JDK: `openjdk version 25.0.1 2025-10-21 LTS`
- Rows: 1000000
- Runs per query: 3 (first discarded as warm-up; reported = min of remaining 2)
- JPointDB time = server-side `elapsedMs`; DuckDB time = `.timer` real

| # | JPointDB, ms | DuckDB, ms | ratio (jp/duck) | query |
|---|-----:|-----:|-----:|:------|
| 01 | 0.614 | 0.800 | 0.77 | `SELECT COUNT(*) FROM hits;` |
| 02 | 1.273 | 2.200 | 0.58 | `SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;` |
| 03 | 4.564 | 0.800 | 5.71 | `SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;` |
| 04 | 1.637 | 2.200 | 0.74 | `SELECT AVG(UserID) FROM hits;` |
| 05 | 5.436 | 2.400 | 2.27 | `SELECT COUNT(DISTINCT UserID) FROM hits;` |
| 06 | 2.764 | 4.000 | 0.69 | `SELECT COUNT(DISTINCT SearchPhrase) FROM hits;` |
| 07 | 2.692 | 0.200 | 13.46 | `SELECT MIN(EventDate), MAX(EventDate) FROM hits;` |
| 08 | 1.492 | 6.600 | 0.23 | `SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDE...` |
| 09 | 9.264 | 1.000 | 9.26 | `SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DES...` |
| 10 | 14.717 | 4.400 | 3.34 | `SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT ...` |
| 11 | 2.638 | 2.800 | 0.94 | `SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <...` |
| 12 | 2.824 | 3.800 | 0.74 | `SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE Mobil...` |
| 13 | 4.367 | 3.000 | 1.46 | `SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPh...` |
| 14 | 5.434 | 2.000 | 2.72 | `SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GRO...` |
| 15 | 4.420 | 0.000 | — | `SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' G...` |
| 16 | 7.048 | 0.400 | 17.62 | `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;` |
| 17 | 9.908 | 0.200 | 49.54 | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY ...` |
| 18 | 9.790 | 2.000 | 4.89 | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY ...` |
| 19 | 77.921 | 2.000 | 38.96 | `SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GR...` |
| 20 | 1.510 | 2.600 | 0.58 | `SELECT UserID FROM hits WHERE UserID = 435090932899640449;` |
| 21 | 7.646 | 0.200 | 38.23 | `SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';` |
| 22 | 8.452 | 5.000 | 1.69 | `SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND Se...` |
| 23 | 7.714 | 1.000 | 7.71 | `SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM h...` |
| 24 | 7.674 | 6.200 | 1.24 | `SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;` |
| 25 | 23.292 | 2.400 | 9.71 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;` |
| 26 | 19.890 | 11.600 | 1.71 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;` |
| 27 | 22.108 | 3.200 | 6.91 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase...` |
| 28 | 5.994 | 1.800 | 3.33 | `SELECT CounterID, AVG(STRLEN(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP ...` |
| 29 | 40.659 | 0.200 | 203.29 | `SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(STRLE...` |
| 30 | 173.433 | 0.400 | 433.58 | `SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(Re...` |
| 31 | 4.904 | 0.400 | 12.26 | `SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FR...` |
| 32 | 9.396 | 3.200 | 2.94 | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits...` |
| 33 | 173.664 | 2.000 | 86.83 | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits...` |
| 34 | 22.295 | 2.200 | 10.13 | `SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;` |
| 35 | 48.865 | 4.000 | 12.22 | `SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;` |
| 36 | 27.326 | 6.000 | 4.55 | `SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROU...` |
| 37 | 22.570 | 1.200 | 18.81 | `SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013...` |
| 38 | 11.958 | 1.000 | 11.96 | `SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '20...` |
| 39 | 9.917 | 2.600 | 3.81 | `SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013...` |
| 40 | 53.355 | 10.600 | 5.03 | `SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND A...` |
| 41 | 12.416 | 3.400 | 3.65 | `SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND Eve...` |
| 42 | 11.418 | 3.600 | 3.17 | `SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE Cou...` |
| 43 | 14.453 | 2.600 | 5.56 | `SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE Coun...` |
| — | **909.712** | **118.200** | **7.70** | _sum_ |

Lower is better. `ratio > 1` means JPointDB is slower than DuckDB on that query.
