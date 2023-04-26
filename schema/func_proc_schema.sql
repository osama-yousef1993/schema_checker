CREATE OR REPLACE FUNCTION tradedAssetsPagination (lim int,pageNum int, sortBy Text ,  direction Text)
RETURNS Table (
	symbol Text,
	display_symbol Text,
	name Text,
	slug Text,
	logo Text,
	temporary_data_delay bool,
	price_24h float,
	percentage_24h float,
	change_value_24h float,
	market_cap float,
	volume_1d float,
	full_count bigint
    ) AS $$
    BEGIN
    RETURN QUERY EXECUTE format('SELECT 
        symbol,
        display_symbol,						  
        name,
        slug,
        logo,
        temporary_data_delay,
        price_24h,
        percentage_24h,
        change_value_24h,						  
        market_cap,
        (nomics::json->>''%s'')::float as volume_1d,
        count(symbol) OVER() AS full_count
        from fundamentalslatest order by %s %s NULLS LAST limit %s offset %s',
                                quote_ident('volume_1d'),
                                sortBy,
                                direction,
                                lim,
                                lim*pageNum
                                ) USING sortBy,lim,pageNum,direction;
    END
    $$ 
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION tradedAssetsPagination_BySource (lim int,pageNum int, sortBy Text ,  direction Text, source text)
RETURNS Table (
	symbol Text,
	display_symbol Text,
	name Text,
	slug Text,
	logo Text,
	temporary_data_delay bool,
	price_24h float,
	percentage_24h float,
	change_value_24h float,
	market_cap float,
	volume_1d float,
	full_count bigint
    ) AS $$
    BEGIN
    RETURN QUERY EXECUTE format('SELECT 
        symbol,
        display_symbol,						  
        name,
        slug,
        logo,
        temporary_data_delay,
        price_24h,
        percentage_24h,
        change_value_24h,						  
        market_cap,
        (nomics::json->>''%s'')::float as volume_1d,
        count(symbol) OVER() AS full_count
        from fundamentalslatest 
        where source = ''%s''
        and name != ''''                                        
        and market_cap is not null
        order by %s %s NULLS LAST limit %s offset %s',
                                quote_ident('volume_1d'),
                                source,
                                sortBy,
                                direction,
                                lim,
                                lim*pageNum
                                ) USING sortBy,lim,pageNum,direction,source;
    END
    $$ 
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION getChartData (intval TEXT,symb TEXT)
RETURNS Table (
    is_index bool, 
    source TEXT, 
    target_resolution_seconds int, 
    prices jsonb,
    symbol TEXT,
    tm_interval TEXT,
    status TEXT
    ) AS $$
    #variable_conflict use_column
    begin
    --If we are not requesting a 24 hour chart return every chart 
    --exluding 24hr. The charts will be used in FDA API
    if intval not like  '%24h%'
    then
    RETURN QUERY select
    is_index, 
    a.source as source , 
    a.target_resolution_seconds as target_resolution_seconds , 
    --append 24 hour cadle to chart that will be returned
    a.prices::jsonb || b.prices::jsonb as prices,
    a.symbol as symbol,
    a.interval as tm_interval,
    c.status as status
    from (
    --get chart for specified interval
    SELECT 
                is_index, 
                source, 
                target_resolution_seconds, 
                prices, 
                symbol,
                interval
            FROM 
                nomics_chart_data
            WHERE 
                target_resolution_seconds != 900
            order by target_resolution_seconds asc
    ) a -- 
    join (
    --get last candle from 24 hr chart
    SELECT symbol, prices->-1 as prices
    FROM   nomics_chart_data 
    where target_resolution_seconds = 900 and symbol = symb) b
    on  b.symbol = a.symbol
    join (select symbol,status from fundamentalslatest where symbol = symb ) c
    on a.symbol = c.symbol;
    --If we are not requesting a 24 hour chart return every chart 
    --exluding 24hr. The charts will be used in FDA API
    else
    RETURN QUERY select
    is_index, 
    a.source as source , 
    a.target_resolution_seconds as target_resolution_seconds , 
    --append 24 hour cadle to chart that will be returned
    a.prices::jsonb as prices,
    a.symbol as symbol,
    a.interval as tm_interval,
    c.status as status
    from (
    --get chart for specified interval
    SELECT 
                is_index, 
                source, 
                target_resolution_seconds, 
                prices, 
                symbol,
                interval
            FROM 
                nomics_chart_data
                where symbol = symb
            order by target_resolution_seconds asc
    ) a 
    join (select symbol,status from fundamentalslatest where symbol = symb ) c
    on a.symbol = c.symbol;

 end if;
 end;
	
    $$
language PLPGSQL;


CREATE OR REPLACE function getcryptocontent (slg text)
RETURNS Table (
	symbol Text,
	display_symbol Text,
	slug Text,
	status Text,
	market_cap float,
	price_24h float,
	number_of_active_market_pairs int,
	description Text,
	name Text,
	website_url Text,
    blog_url Text,
    discord_url Text,
    facebook_url Text,
    github_url Text,
    medium_url Text,
    reddit_url Text,
    telegram_url Text,
    twitter_url Text,
    whitepaper_url Text,
    youtube_url Text,
    bitcointalk_url Text,
    blockexplorer_url Text,
    logo_url Text
    ) 
    as
    $func$
        
        SELECT 
        symbol,
        display_symbol,
        slug,
        status,
        market_cap,
        price_24h,
        number_of_active_market_pairs,
        description,
        name,
        website_url,
        blog_url,
        discord_url,
        facebook_url,
        github_url,
        medium_url,
        reddit_url,
        telegram_url,
        twitter_url,
        whitepaper_url,
        youtube_url,
        bitcointalk_url,
        blockexplorer_url,
        logo_url
    FROM   (SELECT symbol,
                display_symbol,
                slug,
                status,
                market_cap,
                price_24h,
                number_of_active_market_pairs
            FROM   fundamentalslatest
            WHERE  slug = slg) a
        LEFT JOIN (SELECT id,
                            description,
                            name,
                            website_url,
                            blog_url,
                            discord_url,
                            facebook_url,
                            github_url,
                            medium_url,
                            reddit_url,
                            telegram_url,
                            twitter_url,
                            whitepaper_url,
                            youtube_url,
                            bitcointalk_url,
                            blockexplorer_url,
                            logo_url
                    FROM   coingecko_asset_metadata) b
                ON a.symbol = b.id
    $func$
Language sql;


CREATE OR REPLACE FUNCTION getTopExchanges ()
RETURNS Table(
  id text,
	name TEXT,
	year INTEGER,
	description TEXT,
	location TEXT,
	logo_url TEXT,
	website_url TEXT,
	twitter_url TEXT,
	facebook_url TEXT,
	youtube_url TEXT,
	linkedin_url TEXT,
	reddit_url TEXT,
	chat_url TEXT,
	slack_url TEXT,
	telegram_url TEXT,
	blog_url TEXT,
	centralized BOOLEAN,
	decentralized BOOLEAN,
	has_trading_incentive BOOLEAN,
	trust_score INTEGER,
	trust_score_rank INTEGER,
	trade_volume_24h_btc FLOAT,
	trade_volume_24h_btc_normalized FLOAT,
	last_updated TIMESTAMPTZ
    )
    as
    $$
    DECLARE lim int := 5;
    BEGIN
    RETURN QUERY EXECUTE format('
                                SELECT 
                    id as symbol,
                    name as exchange_name, 
                    year as exchange_year, 
                    description, 
                    location, 
                    logo_url, 
                    website_url, 
                    twitter_url, 
                    facebook_url, 
                    youtube_url, 
                    linkedin_url, 
                    reddit_url, 
                    chat_url, 
                    slack_url, 
                    telegram_url, 
                    blog_url, 
                    centralized, 
                    decentralized, 
                    has_trading_incentive, 
                    trust_score, 
                    trust_score_rank, 
                    trade_volume_24h_btc, 
                    trade_volume_24h_btc_normalized, 
                    last_updated
                FROM 
                    public.coingecko_exchange_metadata
                where 
                    trust_score is not null 
                order by trust_score desc
                limit %s;', lim) USING lim; 
        
    END;
    $$
Language plpgsql;


CREATE OR REPLACE FUNCTION getxexchangeidsbytrust ()
RETURNS Table (
	id Text
    ) AS $$
    BEGIN
    RETURN QUERY EXECUTE format('SELECT 
        id
        from coingecko_exchange_metadata 
        where trust_score is not null 
        order by trust_score desc
        limit 5'
                                );
    END
    $$ 
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION getCoinGeckoExchangesList ()
returns Table(id text)
    AS
    $func$
    select id from coingecko_exchanges
    $func$
LANGUAGE sql;

CREATE OR REPLACE PROCEDURE upsertFundamentalsLatest (symbol TEXT,name TEXT,slug TEXT,logo TEXT,float_type TEXT,display_symbol TEXT,original_symbol TEXT,source TEXT,temporary_data_delay BOOLEAN,number_of_active_market_pairs INTEGER,high_24h FLOAT,low_24h FLOAT,high_7d FLOAT,low_7d FLOAT,high_30d FLOAT,low_30d FLOAT,high_1y FLOAT,low_1y FLOAT,high_ytd FLOAT,low_ytd FLOAT,price_24h FLOAT,price_7d FLOAT,price_30d FLOAT,price_1y FLOAT,price_ytd FLOAT,percentage_24h FLOAT,percentage_7d FLOAT,percentage_30d FLOAT,percentage_1y FLOAT,percentage_ytd FLOAT,market_cap FLOAT,market_cap_percent_change_1d FLOAT,market_cap_percent_change_7d FLOAT,market_cap_percent_change_30d FLOAT,market_cap_percent_change_1y FLOAT,market_cap_percent_change_ytd FLOAT,circulating_supply NUMERIC,supply NUMERIC,all_time_low FLOAT,all_time_high FLOAT,date TIMESTAMPTZ,change_value_24h FLOAT,listed_exchange VARCHAR(100)[],market_pairs JSON,exchanges JSON,nomics JSON,forbes JSON,last_updated timestamp, forbes_transparency_volume FLOAT, status TEXT)
LANGUAGE SQL
AS $BODY$
    INSERT INTO fundamentalslatest 

	VALUES (symbol, name, slug, logo, float_type, display_symbol, original_symbol, source, temporary_data_delay, number_of_active_market_pairs, high_24h, low_24h, high_7d, low_7d, high_30d, low_30d, high_1y, low_1y, high_ytd, low_ytd, price_24h, price_7d, price_30d, price_1y, price_ytd, percentage_24h, percentage_7d, percentage_30d, percentage_1y, percentage_ytd,  market_cap, market_cap_percent_change_1d, market_cap_percent_change_7d, market_cap_percent_change_30d, market_cap_percent_change_1y, market_cap_percent_change_ytd, circulating_supply, supply, all_time_low, all_time_high, date, change_value_24h, listed_exchange, market_pairs, exchanges, nomics, forbes,last_updated, forbes_transparency_volume, status) ON CONFLICT (symbol) DO UPDATE SET symbol = EXCLUDED.symbol, name = EXCLUDED.name, slug = EXCLUDED.slug, logo = EXCLUDED.logo, float_type = EXCLUDED.float_type, display_symbol = EXCLUDED.display_symbol, original_symbol = EXCLUDED.original_symbol, source = EXCLUDED.source, temporary_data_delay = EXCLUDED.temporary_data_delay, number_of_active_market_pairs = EXCLUDED.number_of_active_market_pairs, high_24h = EXCLUDED.high_24h, low_24h = EXCLUDED.low_24h, high_7d = EXCLUDED.high_7d, low_7d = EXCLUDED.low_7d, high_30d = EXCLUDED.high_30d, low_30d = EXCLUDED.low_30d, high_1y = EXCLUDED.high_1y, low_1y = EXCLUDED.low_1y, high_ytd = EXCLUDED.high_ytd, low_ytd = EXCLUDED.low_ytd, price_24h = EXCLUDED.price_24h, price_7d = EXCLUDED.price_7d, price_30d = EXCLUDED.price_30d, price_1y = EXCLUDED.price_1y, price_ytd = EXCLUDED.price_ytd,market_cap_percent_change_1d = EXCLUDED.market_cap_percent_change_1d, market_cap_percent_change_7d = EXCLUDED.market_cap_percent_change_7d,market_cap_percent_change_30d  = EXCLUDED.market_cap_percent_change_30d,market_cap_percent_change_1y = EXCLUDED.market_cap_percent_change_1y,market_cap_percent_change_ytd  = EXCLUDED.market_cap_percent_change_ytd, circulating_supply = EXCLUDED.circulating_supply, supply = EXCLUDED.supply, all_time_low = EXCLUDED.all_time_low, all_time_high = EXCLUDED.all_time_high, date = EXCLUDED.date, change_value_24h = EXCLUDED.change_value_24h, listed_exchange = EXCLUDED.listed_exchange, market_pairs = EXCLUDED.market_pairs, exchanges = EXCLUDED.exchanges, nomics = EXCLUDED.nomics, last_updated = EXCLUDED.last_updated, status = EXCLUDED.status, percentage_24h = EXCLUDED.percentage_24h, percentage_7d = EXCLUDED.percentage_7d, percentage_30d = EXCLUDED.percentage_30d, percentage_1y = EXCLUDED.percentage_1y, percentage_ytd = EXCLUDED.percentage_ytd;
$BODY$;

CREATE OR REPLACE PROCEDURE updateChartData (CANDLES JSONB, SYM TEXT) 
AS $$ declare trgt_res_seconds int [] := array [14400,43200,432000,1296000];

  declare intervals int[] := array[ '14400', 
  '43200', 
  '432000', 
  '1296000' ];
  times interval[] := array[ '7 DAYS', 
  '30 DAYS', 
  '1 YEARS', 
  '50 YEARS' ];
  lastInsertedTime TEXT;
  candlesRefined json;
  idx int := 1;
  sec int;
  begin foreach sec in array intervals loop -- get the last time

  -- Get the time from the last inserted candle in the chart data by symbol and target resoltion second
  lastInsertedTime := (
    SELECT 
      prices ->-1 -> 'Time' as timestamp 
    FROM 
      nomics_chart_data 
    where 
      target_resolution_seconds = sec 
      and symbol = sym
  );
  -- compare all of the candles times to the last inserted time
  -- if its > that the las inserted time we will include it in the candlesRefined object.
  -- This helps avoid dupliacates from being entered
  candlesRefined := (
    SELECT 
      json_agg(value) 
    FROM 
      jsonb_array_elements(candles) 
    where 
      cast(
        cast(value -> 'Time' as TEXT) as timestamp
      ) > lastInsertedTime :: timestamp
  );

  if candlesRefined is not NULL 
  then 

  insert into nomics_chart_data -- 3. insert the results into nomics_chart_data  
  select  -- 2. Get all of the information from step 1, and append all of the refinedCandles to it
    is_index, 
    source, 
    target_resolution_seconds, 
    cast(
      json_agg(prices):: jsonb || candlesRefined :: jsonb as json
    ) as prices, 
    symbol, 
    interval 
  from 
    (
      select -- 1. get all prices, and exclude prices that fall out of range 
        is_index, 
        source, 
        target_resolution_seconds, 
        json_array_elements(prices) as prices, 
        symbol, 
        interval 
      from 
        nomics_chart_data 
      where 
        symbol = SYM 
        and target_resolution_seconds = sec
    ) as foo 
  where 
    cast(prices ->> 'Time' as timestamp) >= cast(now() - times[idx] as timestamp) 
  group by 
    is_index, 
    source, 
    target_resolution_seconds, 
    symbol, 
    interval ON CONFLICT(interval) DO 
  UPDATE 
  SET 
    prices = EXCLUDED.prices;
  END IF;
  idx := idx + 1;
  --raise info '%',candlesRefined;
  end loop;
  end;


  $$ LANGUAGE PLPGSQL;

	VALUES (symbol, name, slug, logo, float_type, display_symbol, original_symbol, source, temporary_data_delay, number_of_active_market_pairs, 
    high_24h, low_24h, high_7d, low_7d, high_30d, low_30d, high_1y, low_1y, high_ytd, low_ytd, price_24h, price_7d, price_30d, price_1y, price_ytd, 
    percentage_24h, percentage_7d, percentage_30d, percentage_1y, percentage_ytd, market_cap, 
    market_cap_percent_change_1d, market_cap_percent_change_7d, market_cap_percent_change_30d, market_cap_percent_change_1y, market_cap_percent_change_ytd, 
    circulating_supply, supply, all_time_low, all_time_high, date, change_value_24h, listed_exchange, market_pairs, exchanges, nomics, forbes,last_updated, forbes_transparency_volume) 
    ON CONFLICT (symbol) DO UPDATE SET symbol = EXCLUDED.symbol, name = EXCLUDED.name, slug = EXCLUDED.slug, logo = EXCLUDED.logo, float_type = EXCLUDED.float_type, 
    display_symbol = EXCLUDED.display_symbol, original_symbol = EXCLUDED.original_symbol, source = EXCLUDED.source, temporary_data_delay = EXCLUDED.temporary_data_delay, 
    number_of_active_market_pairs = EXCLUDED.number_of_active_market_pairs, high_24h = EXCLUDED.high_24h, low_24h = EXCLUDED.low_24h, high_7d = EXCLUDED.high_7d, low_7d = EXCLUDED.low_7d, 
    high_30d = EXCLUDED.high_30d, low_30d = EXCLUDED.low_30d, high_1y = EXCLUDED.high_1y, low_1y = EXCLUDED.low_1y, high_ytd = EXCLUDED.high_ytd, low_ytd = EXCLUDED.low_ytd, 
    price_24h = EXCLUDED.price_24h, price_7d = EXCLUDED.price_7d, price_30d = EXCLUDED.price_30d, price_1y = EXCLUDED.price_1y, price_ytd = EXCLUDED.price_ytd,
    percentage_24h = EXCLUDED.percentage_24h, percentage_7d = EXCLUDED.percentage_7d, percentage_30d = EXCLUDED.percentage_30d, 
    percentage_1y = EXCLUDED.percentage_1y, percentage_ytd = EXCLUDED.percentage_ytd, market_cap = EXCLUDED.market_cap, 
    market_cap_percent_change_1d = EXCLUDED.market_cap_percent_change_1d, market_cap_percent_change_7d = EXCLUDED.market_cap_percent_change_7d,
    market_cap_percent_change_30d  = EXCLUDED.market_cap_percent_change_30d,market_cap_percent_change_1y = EXCLUDED.market_cap_percent_change_1y,
    market_cap_percent_change_ytd  = EXCLUDED.market_cap_percent_change_ytd, circulating_supply = EXCLUDED.circulating_supply, 
    supply = EXCLUDED.supply, all_time_low = EXCLUDED.all_time_low, all_time_high = EXCLUDED.all_time_high, 
    date = EXCLUDED.date, change_value_24h = EXCLUDED.change_value_24h, listed_exchange = EXCLUDED.listed_exchange, 
    market_pairs = EXCLUDED.market_pairs, exchanges = EXCLUDED.exchanges, nomics = EXCLUDED.nomics, 
    last_updated = EXCLUDED.last_updated, forbes_transparency_volume= EXCLUDED.forbes_transparency_volume;
$BODY$;

CREATE OR REPLACE PROCEDURE removeEntryFromFundamentalslatest (sym TEXT)
LANGUAGE SQL
AS $BODY$
	delete from fundamentalslatest where symbol ilike sym

$BODY$;

CREATE OR REPLACE PROCEDURE upsert_exchange_fundamentalslatest (name TEXT,slug TEXT,id TEXT,logo TEXT,exchange_active_market_pairs NUMERIC,nomics JSON,forbes JSON, last_updated timestamp)
LANGUAGE SQL
AS $BODY$
  INSERT INTO exchange_fundamentalslatest 
	VALUES (name, slug, id, logo, exchange_active_market_pairs, nomics, forbes, last_updated) 
  ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id, name = EXCLUDED.name, slug = EXCLUDED.slug, 
  logo = EXCLUDED.logo, exchange_active_market_pairs = EXCLUDED.exchange_active_market_pairs,
  nomics = EXCLUDED.nomics, forbes = EXCLUDED.forbes, last_updated = EXCLUDED.last_updated;
$BODY$;

CREATE OR REPLACE PROCEDURE upsertCoingeckoExchanges (IN exchanges coingecko_exchange[]) 
AS 
    $BODY$
    DECLARE
        exchange coingecko_exchange;
    BEGIN
        FOREACH exchange in ARRAY exchanges LOOP 
            INSERT INTO coingecko_exchanges(id, name)
            Values (exchange.id, exchange.name)
            ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id, name =EXCLUDED.name;
        END LOOP;
    END;
$BODY$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE upsertCoingeckoExchangesTickers (IN exchangesTickers coingecko_exchanges_tickers[])
AS 
    $BODY$
    DECLARE
        exchangesTicker coingecko_exchanges_tickers;
    BEGIN
        FOREACH exchangesTicker in ARRAY exchangesTickers LOOP 
            INSERT INTO coingecko_exchanges_tickers(name, tickers)
            VALUES (exchangesTicker.name, exchangesTicker.tickers)
            on conflict (name) DO UPDATE SET name = EXCLUDED.name, tickers = EXCLUDED.tickers;
        END LOOP;
    END;
$BODY$ LANGUAGE plpgsql;
