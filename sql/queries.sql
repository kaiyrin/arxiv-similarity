SET NOCOUNT ON;

SELECT COUNT(*) AS Papers FROM dbo.Papers;
SELECT COUNT(*) AS SimilarPairs FROM dbo.SimilarPairs;

-- Top 20 most similar abstract pairs
SELECT TOP 20
    CAST(p.Jaccard AS DECIMAL(5,3)) AS Jaccard,
    a.PaperId AS PaperA, LEFT(a.Title, 70) AS TitleA,
    b.PaperId AS PaperB, LEFT(b.Title, 70) AS TitleB
FROM dbo.SimilarPairs p
JOIN dbo.Papers a ON a.PaperId = p.PaperAId
JOIN dbo.Papers b ON b.PaperId = p.PaperBId
ORDER BY p.Jaccard DESC;

-- Papers with the most similar matches
SELECT TOP 10 x.PaperId, LEFT(pp.Title, 70) AS Title, COUNT(*) AS Matches
FROM (SELECT PaperAId AS PaperId FROM dbo.SimilarPairs
      UNION ALL
      SELECT PaperBId FROM dbo.SimilarPairs) x
JOIN dbo.Papers pp ON pp.PaperId = x.PaperId
GROUP BY x.PaperId, pp.Title
ORDER BY Matches DESC;
