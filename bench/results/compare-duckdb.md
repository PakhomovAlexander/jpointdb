# JPointDB vs DuckDB — ClickBench side-by-side

- Generated: 2026-04-23 13:45:00 MSK
- Host: `Darwin arm64`
- JDK: `openjdk version 25.0.1 2025-10-21 LTS`
- Rows: 1000000
- Runs per query: 4 (first discarded as warm-up; reported = min of remaining 3)
- JPointDB time = server-side `elapsedMs`; DuckDB time = `.timer` real

| # | JPointDB, ms | DuckDB, ms | ratio (jp/duck) | query |
|---|-----:|-----:|-----:|:------|
| 01 | 0.144 | 0.500 | 0.29 | `SELECT COUNT(*) FROM hits;` |
| 02 | 1.452 | 0.300 | 4.84 | `SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;` |
| 03 | 0.250 | 2.100 | 0.12 | `SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;` |
| 04 | 0.283 | 2.400 | 0.12 | `SELECT AVG(UserID) FROM hits;` |
| 05 | 6.061 | 0.200 | 30.30 | `SELECT COUNT(DISTINCT UserID) FROM hits;` |
| 06 | 3.182 | 5.800 | 0.55 | `SELECT COUNT(DISTINCT SearchPhrase) FROM hits;` |
| 07 | 0.027 | 0.900 | 0.03 | `SELECT MIN(EventDate), MAX(EventDate) FROM hits;` |
| 08 | 1.951 | 2.300 | 0.85 | `SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDE...` |
| 09 | 9.191 | 3.600 | 2.55 | `SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DES...` |
| 10 | 13.960 | 3.000 | 4.65 | `SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT ...` |
| 11 | 1.601 | 0.100 | 16.01 | `SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <...` |
| 12 | 1.628 | 0.500 | 3.26 | `SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE Mobil...` |
| 13 | 2.157 | 0.300 | 7.19 | `SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPh...` |
| 14 | 3.856 | 1.700 | 2.27 | `SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GRO...` |
| 15 | 2.854 | 2.100 | 1.36 | `SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' G...` |
| 16 | 3.988 | 0.200 | 19.94 | `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;` |
| 17 | 5.829 | 1.400 | 4.16 | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY ...` |
| 18 | 5.918 | 1.100 | 5.38 | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY ...` |
| 19 | 15.064 | 2.400 | 6.28 | `SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GR...` |
| 20 | 1.937 | 3.700 | 0.52 | `SELECT UserID FROM hits WHERE UserID = 435090932899640449;` |
| 21 | 1.042 | 1.800 | 0.58 | `SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';` |
| 22 | 1.781 | 0.200 | 8.90 | `SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND Se...` |
| 23 | 2.272 | 0.500 | 4.54 | `SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM h...` |
| 24 | 1.272 | 0.500 | 2.54 | `SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;` |
| 25 | 1.546 | 2.000 | 0.77 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;` |
| 26 | 1.446 | 2.500 | 0.58 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;` |
| 27 | 1.603 | 0.200 | 8.01 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase...` |
| 28 | 5.581 | 0.900 | 6.20 | `SELECT CounterID, AVG(STRLEN(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP ...` |
| 29 | 39.159 | 4.100 | 9.55 | `SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(STRLE...` |
| 30 | 0.314 | 2.600 | 0.12 | `SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(Re...` |
| 31 | 4.182 | 2.800 | 1.49 | `SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FR...` |
| 32 | 8.572 | 1.900 | 4.51 | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits...` |
| 33 | 41.169 | 0.100 | 411.69 | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits...` |
| 34 | 5.776 | 0.300 | 19.25 | `SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;` |
| 35 | 5.747 | 1.700 | 3.38 | `SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;` |
| 36 | 4.515 | 2.100 | 2.15 | `SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROU...` |
| 37 | 15.498 | 3.800 | 4.08 | `SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013...` |
| 38 | 10.370 | 5.100 | 2.03 | `SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '20...` |
| 39 | 8.723 | 1.000 | 8.72 | `SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013...` |
| 40 | 15.561 | 2.400 | 6.48 | `SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND A...` |
| 41 | 14.444 | 10.600 | 1.36 | `SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND Eve...` |
| 42 | 11.972 | 2.900 | 4.13 | `SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE Cou...` |
| 43 | 16.840 | 1.900 | 8.86 | `SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE Coun...` |
| — | **300.718** | **86.500** | **3.48** | _sum_ |

Lower is better. `ratio > 1` means JPointDB is slower than DuckDB on that query.
