select count(*) from sales.hrtable where Region='Asia'
select count(*) from sales.hrtable where Item_Type='Snacks'
select Country, Region, max(Units_Sold) from sales.hrtable group by Country, Region having Region='Europe' limit 1
select Region, max(Units_Sold) from sales.hrtable group by Region limit 1
select Region, max(Total_Revenue) from sales.hrtable group by Region limit 1
select count(*) from sales.hrtable where Region='Europe'