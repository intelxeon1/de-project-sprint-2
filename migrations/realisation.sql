----CREATE TABLES
---shipping country raters
DROP TABLE IF exists public.shipping_country_rates cascade;

CREATE TABLE public.shipping_country_rates (
   shipping_country_id serial ,
   shipping_country            text,
   shipping_country_base_rate  NUMERIC(14,3),
   PRIMARY KEY (shipping_country_id)
);


----shipping_agreement
DROP table if exists public.shipping_agreement cascade;

create table public.shipping_agreement (
	agreementid int4,
	agreement_number text,
	agreement_rate NUMERIC(14,2),
	agreement_commission NUMERIC(14,2),
	primary key (agreementid)
);
drop view if exists public.v_shipping_agreement;
create view public.v_shipping_agreement as (
	select *,
		CONCAT_WS(':',agreementid::text,agreement_number::text,trim(TRAILING '0' from agreement_rate::text),agreement_commission ) as description
		from public.shipping_agreement sa order by description
);

---shipping_transfer_description
drop table if exists public.shipping_transfer cascade;
CREATE TABLE public.shipping_transfer (
   transfer_type_id serial ,
   transfer_type            text,
   transfer_model 			text,
   shipping_transfer_rate   NUMERIC(14,3),
   PRIMARY KEY (transfer_type_id)
);

drop view if exists public.v_shipping_transfer cascade;
create view public.v_shipping_transfer as (
	select *,concat_ws(':',transfer_type,transfer_model) as description  from public.shipping_transfer std 
)
;

----shipping_info 
drop table if exists public.shipping_info cascade;

create table public.shipping_info (
shippingid int8,
vendorid int8,
payment_amount numeric(14, 2) ,
shipping_plan_datetime timestamp,
transfer_type_id int4,
shipping_country_id int4,
agreementid int4,

PRIMARY KEY (shippingid),
foreign key (transfer_type_id) references shipping_transfer(transfer_type_id),
foreign key (shipping_country_id) references shipping_country_rates(shipping_country_id),
foreign key (agreementid) references shipping_agreement(agreementid)
);


---shiping status
drop table if exists public.shipping_status cascade;
create table public.shipping_status (
shippingid int8,
status text ,
state text ,
shipping_start_fact_datetime timestamp NULL,
shipping_end_fact_datetime timestamp NULL,
PRIMARY KEY (shippingid),
)
;
----shipping_datamart
drop table if exists public.shipping_datamart cascade;
create table shipping_datamart (
shippingid int8,
vendorid int8,
transfer_type           text,
is_delay 				int4,
full_day_at_shipping    int4,
is_shipping_finish numeric(1,0),
delay_day_at_shipping  interval null,
payment_amount numeric(14, 2) NULL,
vat numeric(14, 5) null,
profit numeric(14, 4) NULL
)
;
---fill tables
---shipping_country_rates
alter sequence shipping_country_rates_shipping_country_id_seq restart with 1;

truncate table public.shipping_country_rates cascade;
insert into public.shipping_country_rates (shipping_country,shipping_country_base_rate) 
select distinct shipping_country, shipping_country_base_rate from public.shipping order by shipping_country ;
----

----
truncate table public.shipping_agreement cascade;
with cte as (
	select ar[1]::int4,ar[2]::text,ar[3]::numeric(14,2),ar[4]::numeric(14,2) from (
	select distinct regexp_split_to_array(vendor_agreement_description, E'\\:+') as ar 
		from public.shipping s order by 1 
	) as p
	
)
insert into public.shipping_agreement select * from cte;
----

----shipping_transfer
truncate table public.shipping_transfer cascade;
alter sequence shipping_transfer_transfer_type_id_seq restart with 1;

with cte as (
	select ar[1]::text,ar[2]::text, shipping_transfer_rate from (
	select distinct regexp_split_to_array(shipping_transfer_description, E'\\:+') as ar, shipping_transfer_rate
		from public.shipping s order by 1 
	) as p
)
insert into public.shipping_transfer ( transfer_type, transfer_model, shipping_transfer_rate)
select * from cte;
----

---shipping_info
with cte as (
	select distinct shippingid,vendorid, payment_amount,shipping_plan_datetime,
					transfer_type_id, shipping_country_id, agreementid			
	from shipping s 
	left outer join v_shipping_transfer as vstd on s.shipping_transfer_description = vstd.description
	left outer join v_shipping_agreement as vsa on s.vendor_agreement_description = vsa.description 
	left outer join shipping_country_rates as scr on s.shipping_country = scr.shipping_country 
)
insert into public.shipping_info select * from cte;
----

----shipping_status
with cte1 as (
select distinct on (shippingid) shippingid, status,state from public.shipping s order by shippingid, state_datetime desc
),
cte2 as (
select shippingid, max(state_datetime) filter (where state = 'booked') as shipping_start_fact_datetime,
				   max(state_datetime) filter (where state = 'recieved') as shipping_end_fact_datetime
from public.shipping s2 
group by shippingid
)
insert into public.shipping_status 
select cte1.shippingid, status,state,shipping_start_fact_datetime,shipping_end_fact_datetime
from cte1,cte2 where cte1.shippingid = cte2.shippingid
;

---shipping_datamart
with cte as (
select s.shippingid, vendorid,transfer_type,
		extract('day' from shipping_end_fact_datetime - shipping_start_fact_datetime) as full_day_at_shipping ,
		case 
			when shipping_end_fact_datetime >  shipping_plan_datetime then 1
			else 0
		end as is_delay,
		case when status = 'finished' then 1 else 0 end as is_shipping_finish,
		case 
			when shipping_end_fact_datetime >  shipping_plan_datetime then shipping_end_fact_datetime -  shipping_plan_datetime
			else '0'::interval
		end as delay_day_at_shipping,
		payment_amount,
		payment_amount * (shipping_country_base_rate + agreement_rate + shipping_transfer_rate) as vat,
		payment_amount * agreement_commission as profit 
		
	from public.shipping_info s 
	left outer join public.shipping_transfer st on s.transfer_type_id = st.transfer_type_id
	left outer join public.shipping_status ss on s.shippingid = ss.shippingid 
	left outer join public.shipping_country_rates scr on s.shipping_country_id = scr.shipping_country_id
	left outer join public.shipping_agreement sa on s.agreementid = sa.agreementid
)
insert into shipping_datamart select * from cte;



 