{{ @sex := array['男', '女'] }}
{{ @card_type := '身份证' }}
{{ @unit_id := generate_series(0, 10000, 1) }}
{{ @purse_id := generate_series(0, 1000, 1) }}
{{ @rule_id := generate_series(0, 10000, 1) }}
{{ @active_id := generate_series(0, 500, 1) }}

CREATE TABLE `CREDIT_CARD`.`T_CUSTOMER` (
`customer_id` VARCHAR(50) PRIMARY KEY,
    /*{{ @customer_id := rand.uuid() }}*/
`NAME` VARCHAR(50) {{ rand.regex('[0-9a-zA-Z]{1,50}') }},
`phone` VARCHAR(15)  {{ rand.regex('1[0-9]{2}[0-9]{4}[0-9]{4}') }},
`sex` CHAR(1) {{ @sex[rand.zipf(2, 0.8)]  }},
`birthday` DATE {{ TIMESTAMP '1970-01-02 15:04:05' + interval rand.range(0, 50*365*24*60*60) second }},
`card_type` VARCHAR(10) {{ @card_type }},
`card_id` VARCHAR(30) {{ rand.regex('[0-9]{15}|[0-9]{18}') }},
`kaihurq` DATE {{ TIMESTAMP '2016-01-02 15:04:05' }},
`unit_id` VARCHAR(6) {{ @unit_id[rand.zipf(9999, 0.5)] }},
`dept_id` VARCHAR(6) {{ @unit_id[rand.zipf(9999, 0.5)] }},
`vip_level` CHAR(1) {{ rand.regex('[0-4]{1,1}') }},
`card_num` int UNSIGNED NOT NULL,
 /*{{ @card_num := rand.range_inclusive(0, 4) }}*/
`purse_num` int UNSIGNED NOT NULL
  /*{{ @purse_num := rand.range_inclusive(0, 10) }}*/
);