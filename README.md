```
SELECT 
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE old_reference_number IS NOT NULL) AS non_null_old_ref,
    COUNT(*) FILTER (WHERE old_reference_number IS NULL) AS null_old_ref
FROM deposit.test_recon_obs_time_deposit_data;
```
```
SELECT 
    COUNT(*) AS rollover_rows_before,
    COUNT(DISTINCT reference_number) AS unique_reference_numbers
FROM deposit.test_recon_time_deposit_rollover;
```
```
SELECT reference_number
FROM deposit.test_recon_time_deposit_rollover
ORDER BY reference_number
LIMIT 10;
```
