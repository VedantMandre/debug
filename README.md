```
CREATE OR REPLACE PROCEDURE deposit.sync_time_deposit_rollover()
LANGUAGE plpgsql
AS $$
DECLARE
    v_matched_count INTEGER := 0;
    v_debug_info TEXT := '';
BEGIN
    -- Debugging: Log initial state
    RAISE NOTICE 'Starting time deposit rollover synchronization...';

    -- Step 1: Update existing matched records
    WITH matched_records AS (
        SELECT 
            otd.trade_number AS new_trade_number,
            otd.old_reference_number AS matched_reference_number,
            otd.time_deposit_amount AS new_principal_amount,
            otd.maturity_date AS new_maturity_date,
            otd.currency AS new_currency_code,
            otd.interest_accrued_till_date AS new_accrued_interest,
            otd.interest_at_maturity AS new_interest_amount,
            otd.branch AS new_branch_code,
            otd.funding_source AS new_funding_source,
            otd.obs_code AS new_obs_number,
            otd.time_deposit_account_number AS new_account_number,
            otd.settlement_account_number AS new_settlement_account,
            otd.maturity_status AS new_maturity_status
        FROM deposit.test_recon_obs_time_deposit_data otd
        INNER JOIN deposit.test_recon_time_deposit_rollover tdr
            ON otd.old_reference_number = tdr.reference_number
        WHERE otd.old_reference_number IS NOT NULL
    )
    UPDATE deposit.test_recon_time_deposit_rollover tdr
    SET 
        trade_number = mr.new_trade_number,
        principal_amount = mr.new_principal_amount,
        maturity_date = mr.new_maturity_date,
        currency_code = mr.new_currency_code,
        accrued_interest = mr.new_accrued_interest,
        interest_amount = mr.new_interest_amount,
        branch_code = mr.new_branch_code,
        funding_source = mr.new_funding_source,
        obs_number = mr.new_obs_number,
        account_number = mr.new_account_number,
        settlement_account = mr.new_settlement_account,
        maturity_status = mr.new_maturity_status,
        status = 'Finalized'
    FROM matched_records mr
    WHERE tdr.reference_number = mr.matched_reference_number;

    -- Get count of matched and updated records
    GET DIAGNOSTICS v_matched_count = ROW_COUNT;

    -- Step 2: Prepare debug information
    v_debug_info := format(
        'Synchronization Summary: %s records matched and updated', 
        v_matched_count
    );

    -- Log debug information
    RAISE NOTICE '%', v_debug_info;

    -- Confirmation Message
    RAISE NOTICE 'Time deposit rollover synchronization completed successfully!';
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
