DELETE FROM spots 
WHERE BBG='^FCHI'

SELECT COUNT(*) AS nbr_doublon, BBG, date, spot, flag
FROM spots
GROUP BY BBG, date, spot, flag
HAVING COUNT(*) > 1
