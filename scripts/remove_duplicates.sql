DELETE FROM transactions a USING (
    SELECT min(ctid) as ctid, signature
    FROM transactions
    GROUP BY signature
    HAVING count(signature) > 1
  ) b
  WHERE a.signature = b.signature
  AND a.ctid <> b.ctid ;
