WITH opportunity_to_name AS
(
    SELECT  -- make sure there is only one name per id
    id AS account_id,
    name AS account_name
    FROM
    sfdc.accounts
    WHERE
    dt = '{run_date}'
    -- dt = '2019-08-07'
    GROUP BY
    id,
    name
),

SELECT
*
FROM
arr_clean