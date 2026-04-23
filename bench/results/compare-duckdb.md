# JPointDB vs DuckDB — ClickBench side-by-side

- Generated: 2026-04-23 17:49:33 MSK
- Host: `Darwin arm64`
- JDK: `openjdk version 25.0.1 2025-10-21 LTS`
- Rows: 1000000
- Runs per query: 4 (first discarded as warm-up; reported = min of remaining 3)
- JPointDB time = server-side `elapsedMs`; DuckDB time = `.timer` real

| # | JPointDB, ms | DuckDB, ms | ratio (jp/duck) | query |
|---|-----:|-----:|-----:|:------|
| 01 | 0.161 | 0.400 | 0.40 | `SELECT COUNT(*) FROM hits;` |
| 02 | 0.552 | 0.300 | 1.84 | `SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;` |
| 03 | 0.290 | 2.100 | 0.14 | `SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;` |
| 04 | 0.318 | 2.400 | 0.13 | `SELECT AVG(UserID) FROM hits;` |
| 05 | 3.749 | 0.200 | 18.75 | `SELECT COUNT(DISTINCT UserID) FROM hits;` |
| 06 | 3.898 | 5.800 | 0.67 | `SELECT COUNT(DISTINCT SearchPhrase) FROM hits;` |
| 07 | 0.053 | 1.000 | 0.05 | `SELECT MIN(EventDate), MAX(EventDate) FROM hits;` |
| 08 | 0.443 | 2.400 | 0.18 | `SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDE...` |
| 09 | 4.914 | 3.500 | 1.40 | `SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DES...` |
| 10 | 6.027 | 3.000 | 2.01 | `SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT ...` |
| 11 | 1.274 | 0.100 | 12.74 | `SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <...` |
| 12 | 1.249 | 0.500 | 2.50 | `SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE Mobil...` |
| 13 | 1.942 | 0.300 | 6.47 | `SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPh...` |
| 14 | 3.461 | 1.800 | 1.92 | `SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GRO...` |
| 15 | 2.769 | 2.200 | 1.26 | `SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' G...` |
| 16 | 3.930 | 0.200 | 19.65 | `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;` |
| 17 | 5.801 | 1.400 | 4.14 | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY ...` |
| 18 | 5.932 | 1.100 | 5.39 | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY ...` |
| 19 | 11.348 | 2.300 | 4.93 | `SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GR...` |
| 20 | 0.137 | 3.900 | 0.04 | `SELECT UserID FROM hits WHERE UserID = 435090932899640449;` |
| 21 | 1.214 | 1.900 | 0.64 | `SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';` |
| 22 | 1.608 | 0.100 | 16.08 | `SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND Se...` |
| 23 | 1.551 | 0.500 | 3.10 | `SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM h...` |
| 24 | 1.269 | 0.300 | 4.23 | `SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;` |
| 25 | 1.608 | 1.900 | 0.85 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;` |
| 26 | 1.563 | 2.600 | 0.60 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;` |
| 27 | 1.637 | 0.200 | 8.18 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase...` |
| 28 | 2.400 | 1.000 | 2.40 | `SELECT CounterID, AVG(STRLEN(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP ...` |
| 29 | 10.446 | 4.200 | 2.49 | `SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(STRLE...` |
| 30 | 0.328 | 2.700 | 0.12 | `SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(Re...` |
| 31 | 3.697 | 2.900 | 1.27 | `SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FR...` |
| 32 | 7.155 | 1.900 | 3.77 | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits...` |
| 33 | 29.508 | 0.200 | 147.54 | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits...` |
| 34 | 4.921 | 0.300 | 16.40 | `SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;` |
| 35 | 5.707 | 2.000 | 2.85 | `SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;` |
| 36 | 4.191 | 2.000 | 2.10 | `SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROU...` |
| 37 | 14.370 | 4.100 | 3.50 | `SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013...` |
| 38 | 9.184 | 5.100 | 1.80 | `SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '20...` |
| 39 | 2.250 | 0.900 | 2.50 | `SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013...` |
| 40 | 12.552 | 2.700 | 4.65 | `SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND A...` |
| 41 | 11.254 | 11.700 | 0.96 | `SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND Eve...` |
| 42 | 3.193 | 2.900 | 1.10 | `SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE Cou...` |
| 43 | 2.892 | 2.200 | 1.31 | `SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE Coun...` |
| — | **192.746** | **89.200** | **2.16** | _sum_ |

Lower is better. `ratio > 1` means JPointDB is slower than DuckDB on that query.
