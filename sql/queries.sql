SET NOCOUNT ON;

SELECT COUNT(*) AS Papers FROM dbo. Papers;
SELECT COUNT(*) AS SimilarPairs FROM dbo.SimilarPairs;
SELECT SUM(CASE WHEN Jaccard = 1 THEN 1 ELSE 0 END) AS IdenticalPairs,
       SUM(CASE WHEN Jaccard < 1 THEN 1 ELSE 0 END) AS NearDuplicatePairs
FROM dbo.SimilarPairs;

-- Near-duplicates: similar but not identical abstracts (the interesting LSH finds)
SELECT TOP 20
    CAST(p.Jaccard AS DECIMAL(5,3)) AS Jaccard,
    a.PaperId AS PaperA, LEFT(REPLACE(REPLACE(a.Title, CHAR(13), ' '), CHAR(10), ' '), 60) AS TitleA,
    b.PaperId AS PaperB, LEFT(REPLACE(REPLACE(b.Title, CHAR(13), ' '), CHAR(10), ' '), 60) AS TitleB
FROM dbo.SimilarPairs p
JOIN dbo.Papers a ON a.PaperId = p.PaperAId
JOIN dbo.Papers b ON b.PaperId = p.PaperBId
WHERE p.Jaccard < 1
ORDER BY p.Jaccard DESC;

-- Papers with the most matches (clusters of duplicates)
SELECT TOP 10 x.PaperId,
       LEFT(REPLACE(REPLACE(pp.Title, CHAR(13), ' '), CHAR(10), ' '), 60) AS Title,
       COUNT(*) AS Matches
FROM (SELECT PaperAId AS PaperId FROM dbo.SimilarPairs
      UNION ALL
      SELECT PaperBId FROM dbo.SimilarPairs) x
JOIN dbo.Papers pp ON pp.PaperId = x.PaperId
GROUP BY x.PaperId, pp.Title
ORDER BY Matches DESC;
