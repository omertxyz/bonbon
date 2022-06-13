SELECT b.*, g.*
FROM bonbons b
JOIN glazings g ON (b.metadata_key = g.metadata_key)
LEFT OUTER JOIN glazings g2 ON (b.metadata_key = g2.metadata_key AND
    ((g.slot, g.block_index, g.outer_index, g.inner_index)
     < (g2.slot, g2.block_index, g2.outer_index, g2.inner_index)))
LIMIT 5;
