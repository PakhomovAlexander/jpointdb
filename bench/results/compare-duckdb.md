# JPointDB vs DuckDB — ClickBench side-by-side

- Generated: 2026-04-23 16:21:48 MSK
- Host: `Darwin arm64`
- JDK: `openjdk version 25.0.1 2025-10-21 LTS`
- Rows: 1000000
- Runs per query: 4 (first discarded as warm-up; reported = min of remaining 3)
- JPointDB time = server-side `elapsedMs`; DuckDB time = `.timer` real

| # | JPointDB, ms | DuckDB, ms | ratio (jp/duck) | query |
|---|-----:|-----:|-----:|:------|
| 01 | 0.133 | 0.500 | 0.27 | `SELECT COUNT(*) FROM hits;` |
| 02 | 0.586 | 0.200 | 2.93 | `SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0;` |
| 03 | 0.277 | 2.100 | 0.13 | `SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits;` |
| 04 | 0.303 | 2.400 | 0.13 | `SELECT AVG(UserID) FROM hits;` |
| 05 | 7.775 | 0.100 | 77.75 | `SELECT COUNT(DISTINCT UserID) FROM hits;` |
| 06 | 2.912 | 5.900 | 0.49 | `SELECT COUNT(DISTINCT SearchPhrase) FROM hits;` |
| 07 | 0.055 | 0.900 | 0.06 | `SELECT MIN(EventDate), MAX(EventDate) FROM hits;` |
| 08 | 1.109 | 2.500 | 0.44 | `SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDE...` |
| 09 | 9.349 | 3.600 | 2.60 | `SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DES...` |
| 10 | 14.144 | 2.900 | 4.88 | `SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT ...` |
| 11 | 1.304 | 0.200 | 6.52 | `SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <...` |
| 12 | 1.254 | 0.500 | 2.51 | `SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE Mobil...` |
| 13 | 2.555 | 0.300 | 8.52 | `SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPh...` |
| 14 | 3.992 | 1.800 | 2.22 | `SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GRO...` |
| 15 | 2.775 | 2.200 | 1.26 | `SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' G...` |
| 16 | 4.771 | 0.200 | 23.85 | `SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;` |
| 17 | 6.239 | 1.400 | 4.46 | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY ...` |
| 18 | 6.395 | 1.000 | 6.39 | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY ...` |
| 19 | 11.763 | 2.300 | 5.11 | `SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GR...` |
| 20 | 0.147 | 3.600 | 0.04 | `SELECT UserID FROM hits WHERE UserID = 435090932899640449;` |
| 21 | 1.372 | 1.800 | 0.76 | `SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%';` |
| 22 | 1.194 | 0.000 | — | `SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND Se...` |
| 23 | 1.487 | 0.500 | 2.97 | `SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM h...` |
| 24 | 1.300 | 0.300 | 4.33 | `SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10;` |
| 25 | 1.674 | 1.900 | 0.88 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10;` |
| 26 | 1.555 | 2.500 | 0.62 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10;` |
| 27 | 1.774 | 0.200 | 8.87 | `SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase...` |
| 28 | 6.129 | 0.900 | 6.81 | `SELECT CounterID, AVG(STRLEN(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP ...` |
| 29 | 10.848 | 4.100 | 2.65 | `SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(STRLE...` |
| 30 | 0.311 | 2.500 | 0.12 | `SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(Re...` |
| 31 | 3.689 | 2.900 | 1.27 | `SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FR...` |
| 32 | 8.434 | 1.800 | 4.69 | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits...` |
| 33 | 36.302 | 0.100 | 363.02 | `SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits...` |
| 34 | 4.585 | 0.200 | 22.92 | `SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10;` |
| 35 | 4.662 | 1.800 | 2.59 | `SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10;` |
| 36 | 4.823 | 1.900 | 2.54 | `SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROU...` |
| 37 | 13.663 | 4.000 | 3.42 | `SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013...` |
| 38 | 8.835 | 5.000 | 1.77 | `SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '20...` |
| 39 | 2.502 | 0.800 | 3.13 | `SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013...` |
| 40 | 12.699 | 2.300 | 5.52 | `SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND A...` |
| 41 | 9.924 | 11.200 | 0.89 | `SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND Eve...` |
| 42 | 3.419 | 2.900 | 1.18 | `SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE Cou...` |
| 43 | 2.945 | 1.900 | 1.55 | `SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE Coun...` |
| — | **221.964** | **86.100** | **2.58** | _sum_ |

Lower is better. `ratio > 1` means JPointDB is slower than DuckDB on that query.
