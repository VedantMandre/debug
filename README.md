```
CREATE OR REPLACE PROCEDURE deposit.sync_time_deposit_rollover()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Step 1: Debugging - Check which old_reference_numbers already exist in the rollover table
    RAISE NOTICE 'Checking existing references...';
    
    FOR rec IN 
        SELECT 
            TRIM(otd.old_reference_number) AS old_reference_number, 
            tdr.reference_number
        FROM deposit.test_recon_obs_time_deposit_data otd
        LEFT JOIN deposit.test_recon_time_deposit_rollover tdr
        ON TRIM(otd.old_reference_number) = TRIM(tdr.reference_number)
        WHERE otd.old_reference_number IS NOT NULL
        AND tdr.reference_number IS NOT NULL
    LOOP
        IF rec.reference_number IS NOT NULL THEN
            RAISE NOTICE 'Match found: old_reference_number = %, reference_number = %', 
                         rec.old_reference_number, rec.reference_number;
        ELSE
            RAISE NOTICE 'No match: old_reference_number = %', 
                         rec.old_reference_number;
        END IF;
    END LOOP;

    -- Step 2: Update existing records where old_reference_number matches reference_number
    RAISE NOTICE 'Updating existing matched records...';
    
    WITH rolled_over_data AS (
        SELECT 
            otd.trade_number, 
            TRIM(otd.old_reference_number) AS reference_number,  
            otd.time_deposit_amount AS principal_amount, 
            otd.maturity_date, 
            otd.currency AS currency_code, 
            otd.interest_accrued_till_date AS accrued_interest, 
            otd.interest_at_maturity AS interest_amount, 
            otd.branch AS branch_code, 
            otd.funding_source, 
            otd.obs_code AS obs_number, 
            otd.time_deposit_account_number AS account_number, 
            otd.settlement_account_number AS settlement_account, 
            otd.maturity_status, 
            'Finalized' AS status
        FROM deposit.test_recon_obs_time_deposit_data otd
        WHERE otd.old_reference_number IS NOT NULL
    )
    
    UPDATE deposit.test_recon_time_deposit_rollover tdr
    SET 
        trade_number = rod.trade_number,
        principal_amount = rod.principal_amount,
        maturity_date = rod.maturity_date,
        currency_code = rod.currency_code,
        accrued_interest = rod.accrued_interest,
        interest_amount = rod.interest_amount,
        branch_code = rod.branch_code,
        funding_source = rod.funding_source,
        obs_number = rod.obs_number,
        account_number = rod.account_number,
        settlement_account = rod.settlement_account,
        maturity_status = rod.maturity_status,
        status = 'Finalized'
    FROM rolled_over_data rod
    WHERE TRIM(tdr.reference_number) = rod.reference_number
    AND tdr.reference_number IS NOT NULL;

    RAISE NOTICE 'Number of rows updated: %', SQL%ROWCOUNT;

    -- Step 3: Insert new rollovers (where old_reference_number doesn't exist in target)
    RAISE NOTICE 'Inserting new rollovers...';
    
    WITH rolled_over_data AS (
        SELECT 
            otd.trade_number, 
            TRIM(otd.old_reference_number) AS reference_number,  
            otd.time_deposit_amount AS principal_amount, 
            otd.maturity_date, 
            otd.currency AS currency_code, 
            otd.interest_accrued_till_date AS accrued_interest, 
            otd.interest_at_maturity AS interest_amount, 
            otd.branch AS branch_code, 
            otd.funding_source, 
            otd.obs_code AS obs_number, 
            otd.time_deposit_account_number AS account_number, 
            otd.settlement_account_number AS settlement_account, 
            otd.maturity_status, 
            'Finalized' AS status
        FROM deposit.test_recon_obs_time_deposit_data otd
        WHERE otd.old_reference_number IS NOT NULL
    )
    
    INSERT INTO deposit.test_recon_time_deposit_rollover (
        trade_number, reference_number, principal_amount, 
        maturity_date, currency_code, accrued_interest, 
        interest_amount, branch_code, funding_source, 
        obs_number, account_number, settlement_account, 
        maturity_status, status
    )
    SELECT 
        rod.trade_number, rod.reference_number, rod.principal_amount, 
        rod.maturity_date, rod.currency_code, rod.accrued_interest, 
        rod.interest_amount, rod.branch_code, rod.funding_source, 
        rod.obs_number, rod.account_number, rod.settlement_account, 
        rod.maturity_status, rod.status
    FROM rolled_over_data rod
    WHERE rod.reference_number IS NOT NULL  -- Ensure no NULL reference_number values are inserted
    AND NOT EXISTS (
        SELECT 1 
        FROM deposit.test_recon_time_deposit_rollover tdr
        WHERE TRIM(tdr.reference_number) = rod.reference_number
    );

    RAISE NOTICE 'Number of rows inserted: %', SQL%ROWCOUNT;

    -- Confirmation Message
    RAISE NOTICE 'Sync completed successfully!';
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error occurred: %', SQLERRM;
        RAISE;
END;
$$;
```
```
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT reference_number) AS unique_reference_numbers,
    COUNT(*) FILTER (WHERE reference_number IS NULL) AS null_reference_numbers,
    COUNT(*) FILTER (WHERE status = 'Finalized') AS updated_rows
FROM deposit.test_recon_time_deposit_rollover;
```
```
CREATE OR REPLACE PROCEDURE deposit.sync_time_deposit_rollover()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Step 1: Debugging - Check which old_reference_numbers already exist in the rollover table
    RAISE NOTICE 'Checking existing references...';
    
    FOR rec IN 
        SELECT otd.old_reference_number, tdr.reference_number
        FROM deposit.test_recon_obs_time_deposit_data otd
        LEFT JOIN deposit.test_recon_time_deposit_rollover tdr
        ON otd.old_reference_number = tdr.reference_number
        WHERE otd.old_reference_number IS NOT NULL
    LOOP
        IF rec.reference_number IS NOT NULL THEN
            RAISE NOTICE 'Match found: old_reference_number = %, reference_number = %', 
                         rec.old_reference_number, rec.reference_number;
        ELSE
            RAISE NOTICE 'No match: old_reference_number = %', 
                         rec.old_reference_number;
        END IF;
    END LOOP;

    -- Step 2: Update existing records where old_reference_number matches reference_number
    RAISE NOTICE 'Updating existing matched records...';
    
    WITH rolled_over_data AS (
        SELECT 
            otd.trade_number, 
            otd.old_reference_number AS reference_number,  
            otd.time_deposit_amount AS principal_amount, 
            otd.maturity_date, 
            otd.currency AS currency_code, 
            otd.interest_accrued_till_date AS accrued_interest, 
            otd.interest_at_maturity AS interest_amount, 
            otd.branch AS branch_code, 
            otd.funding_source, 
            otd.obs_code AS obs_number, 
            otd.time_deposit_account_number AS account_number, 
            otd.settlement_account_number AS settlement_account, 
            otd.maturity_status, 
            'Finalized' AS status
        FROM deposit.test_recon_obs_time_deposit_data otd
        WHERE otd.old_reference_number IS NOT NULL
    )
    
    UPDATE deposit.test_recon_time_deposit_rollover tdr
    SET 
        trade_number = rod.trade_number,
        principal_amount = rod.principal_amount,
        maturity_date = rod.maturity_date,
        currency_code = rod.currency_code,
        accrued_interest = rod.accrued_interest,
        interest_amount = rod.interest_amount,
        branch_code = rod.branch_code,
        funding_source = rod.funding_source,
        obs_number = rod.obs_number,
        account_number = rod.account_number,
        settlement_account = rod.settlement_account,
        maturity_status = rod.maturity_status,
        status = 'Finalized'
    FROM rolled_over_data rod
    WHERE tdr.reference_number = rod.reference_number;

    -- Step 3: Log the number of updated rows
    RAISE NOTICE 'Number of rows updated: %', SQL%ROWCOUNT;

    -- Confirmation Message
    RAISE NOTICE 'Sync completed successfully!';
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error occurred: %', SQLERRM;
        RAISE;
END;
$$;
```
