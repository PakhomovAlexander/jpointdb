# JPointDB vs DuckDB — ClickBench side-by-side

- Generated: 2026-04-21 14:06:51 MSK
- Host: `Darwin arm64`
- JDK: `openjdk version 25.0.1 2025-10-21 LTS`
- Rows: 1000000
- Runs per query: 3 (first discarded as warm-up; reported = min of remaining 2)
- JPointDB time = server-side `elapsedMs`; DuckDB time = `.timer` real

| # | JPointDB, ms | DuckDB, ms | ratio (jp/duck) | query |
|---|-----:|-----:|-----:|:------|
| 01 | 0.904 | 0.400 | 2.26 | `SELECT COUNT(*) FROM hits;` |
| 02 | 2.095 | 2.200 | 0.95 | `SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;` |
| 03 | 3.463 | 0.600 | 5.77 | `SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;` |
| 04 | 1.878 | 2.200 | 0.85 | `SELECT AVG(UserID) FROM hits;` |
| 05 | 5.550 | 2.400 | 2.31 | `SELECT COUNT(DISTINCT UserID) FROM hits;` |
| 06 | 2.933 | 4.000 | 0.73 | `SELECT COUNT(DISTINCT SearchPhrase) FROM hits;` |
| 07 | 3.166 | 0.200 | 15.83 | `SELECT MIN(EventDate), MAX(EventDate) FROM hits;` |
| 08 | 2.142 | 5.400 | 0.40 | `SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDE...` |
| 09 | 10.064 | 1.000 | 10.06 | `SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DES...` |
| 10 | 14.462 | 4.200 | 3.44 | `SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT ...` |
| 11 | 2.810 | 2.600 | 1.08 | `SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <...` |
| 12 | 2.912 | 3.600 | 0.81 | `SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE Mobil...` |
| 13 | 3.858 | 2.800 | 1.38 | `SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPh...` |
| 14 | 5.275 | 2.000 | 2.64 | `SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GRO...` |
| 15 | 4.561 | 0.200 | 22.80 | `SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' G...` |
| 16 | 6.060 | 0.400 | 15.15 | `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;` |
| 17 | 9.690 | 0.400 | 24.22 | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY ...` |
| 18 | 9.657 | 1.800 | 5.37 | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY ...` |
| 19 | 118.741 | 2.000 | 59.37 | `SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GR...` |
| 20 | 2.242 | 2.400 | 0.93 | `SELECT UserID FROM hits WHERE UserID = 435090932899640449;` |
| 21 | 7.540 | 0.200 | 37.70 | `SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';` |
| 22 | 8.259 | 5.000 | 1.65 | `SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND Se...` |
| 23 | 7.901 | 1.000 | 7.90 | `SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM h...` |
| 24 | 7.511 | 6.200 | 1.21 | `SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;` |
| 25 | 22.527 | 2.400 | 9.39 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;` |
| 26 | 20.930 | 11.600 | 1.80 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;` |
| 27 | 24.266 | 3.000 | 8.09 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase...` |
| 28 | 7.019 | 1.800 | 3.90 | `SELECT CounterID, AVG(STRLEN(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP ...` |
| 29 | 40.999 | 0.200 | 205.00 | `SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(STRLE...` |
| 30 | 0.295 | 0.400 | 0.74 | `SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(Re...` |
| 31 | 4.911 | 0.400 | 12.28 | `SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FR...` |
| 32 | 9.269 | 3.200 | 2.90 | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits...` |
| 33 | 222.488 | 2.000 | 111.24 | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits...` |
| 34 | 24.993 | 2.200 | 11.36 | `SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;` |
| 35 | 59.771 | 4.000 | 14.94 | `SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;` |
| 36 | 28.959 | 6.000 | 4.83 | `SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROU...` |
| 37 | 23.944 | 1.400 | 17.10 | `SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013...` |
| 38 | 14.464 | 1.200 | 12.05 | `SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '20...` |
| 39 | 11.681 | 2.600 | 4.49 | `SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013...` |
| 40 | 57.845 | 10.600 | 5.46 | `SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND A...` |
| 41 | 14.991 | 3.600 | 4.16 | `SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND Eve...` |
| 42 | 13.918 | 3.600 | 3.87 | `SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE Cou...` |
| 43 | 17.258 | 2.000 | 8.63 | `SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE Coun...` |
| — | **864.202** | **115.400** | **7.49** | _sum_ |

Lower is better. `ratio > 1` means JPointDB is slower than DuckDB on that query.
