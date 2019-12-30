use adsurgdb;

drop table if exists africa;

create table africa (
	Country_Name varchar(255) null,
	Country_Code varchar(55) null,
    Series_Name varchar(255) null,
    Series_Code varchar(100) null,
    YR1960 varchar(55) null,
    YR1961 varchar(55) null,
    YR1962 varchar(55) null,
    YR1963 varchar(55) null,
    YR1964 varchar(55) null,
    YR1965 varchar(55) null,
    YR1966 varchar(55) null,
    YR1967 varchar(55) null,
    YR1968 varchar(55) null,
    YR1969 varchar(55) null,
		YR1970 varchar(55) null,
    YR1971 varchar(55) null,
    YR1972 varchar(55) null,
    YR1973 varchar(55) null,
    YR1974 varchar(55) null,
    YR1975 varchar(55) null,
    YR1976 varchar(55) null,
    YR1977 varchar(55) null,
    YR1978 varchar(55) null,
    YR1979 varchar(55) null,
		YR1980 varchar(55) null,
    YR1981 varchar(55) null,
    YR1982 varchar(55) null,
    YR1983 varchar(55) null,
    YR1984 varchar(55) null,
    YR1985 varchar(55) null,
    YR1986 varchar(55) null,
    YR1987 varchar(55) null,
    YR1988 varchar(55) null,
    YR1989 varchar(55) null,
		YR1990 varchar(55) null,
    YR1991 varchar(55) null,
    YR1992 varchar(55) null,
    YR1993 varchar(55) null,
    YR1994 varchar(55) null,
    YR1995 varchar(55) null,
    YR1996 varchar(55) null,
    YR1997 varchar(55) null,
    YR1998 varchar(55) null,
    YR1999 varchar(55) null,
		YR2000 varchar(55) null,
    YR2001 varchar(55) null,
    YR2002 varchar(55) null,
    YR2003 varchar(55) null,
    YR2004 varchar(55) null,
    YR2005 varchar(55) null,
    YR2006 varchar(55) null,
    YR2007 varchar(55) null,
    YR2008 varchar(55) null,
    YR2009 varchar(55) null,
		YR2010 varchar(55) null,
    YR2011 varchar(55) null,
    YR2012 varchar(55) null,
    YR2013 varchar(55) null,
    YR2014 varchar(55) null,
    YR2015 varchar(55) null,
    YR2016 varchar(55) null,
    YR2017 varchar(55) null,
    YR2018 varchar(55) null,
    YR2019 varchar(55) null
    );

GRANT LOAD FROM S3 ON *.* TO jaguar@localhost;

LOAD DATA FROM S3 FILE 's3://advancedsurgicare/individual_african_countries.csv'
INTO TABLE adsurgdb.africa
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
(@v1,@v2,@v3,@v4,
	@v60,@v61,@v62,@v63,@v64,@v65,@v66,@v67,@v68,@v69,
	@v70,@v71,@v72,@v73,@v74,@v75,@v76,@v77,@v78,@v79,
	@v80,@v81,@v82,@v83,@v84,@v85,@v86,@v87,@v88,@v89,
	@v90,@v91,@v92,@v93,@v94,@v95,@v96,@v97,@v98,@v99,
	@v101,@v101,@v102,@v103,@v104,@v105,@v106,@v107,@v108,@v109,
	@v110,@v111,@v112,@v113,@v114,@v115,@v116,@v117,@v118,@v119)
SET Country_Name = @v1,
	Country_Code = @v2,
	Series_Name = @v3,
	Series_Code = @v4,
	YR1960 = @v60,
	YR1961 = @v61,
	YR1962 = @v62,
	YR1963 = @v63,
	YR1964 = @v64,
	YR1965 = @v65,
	YR1966 = @v66,
	YR1967 = @v67,
	YR1968 = @v68,
	YR1969 = @v69,
	YR1970 = @v70,
	YR1971 = @v71,
	YR1972 = @v72,
	YR1973 = @v73,
	YR1974 = @v74,
	YR1975 = @v75,
	YR1976 = @v76,
	YR1977 = @v77,
	YR1978 = @v78,
	YR1979 = @v79,
	YR1980 = @v80,
	YR1981 = @v81,
	YR1982 = @v82,
	YR1983 = @v83,
	YR1984 = @v84,
	YR1985 = @v85,
	YR1986 = @v86,
	YR1987 = @v87,
	YR1988 = @v88,
	YR1989 = @v89,
	YR1980 = @v80,
	YR1991 = @v91,
	YR1992 = @v92,
	YR1993 = @v93,
	YR1994 = @v94,
	YR1995 = @v95,
	YR1996 = @v96,
	YR1997 = @v97,
	YR1998 = @v98,
	YR1999 = @v99,
	YR2001 = @v101,
	YR2002 = @v102,
	YR2003 = @v103,
	YR2004 = @v104,
	YR2005 = @v105,
	YR2006 = @v106,
	YR2007 = @v107,
	YR2008 = @v108,
	YR2009 = @v109,
	YR2010 = @v110,
	YR2011 = @v111,
	YR2012 = @v112,
	YR2013 = @v113,
	YR2014 = @v114,
	YR2015 = @v115,
	YR2016 = @v116,
	YR2017 = @v117,
	YR2018 = @v118,
	YR2019 = @v119;



select Country_Name,
    1960 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1960 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1960 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1960 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1960 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1960 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1960 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1960 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1960 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1960 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1961 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1961 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1961 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1961 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1961 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1961 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1961 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1961 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1961 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1961 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1962 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1962 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1962 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1962 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1962 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1962 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1962 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1962 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1962 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1962 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1963 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1963 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1963 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1963 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1963 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1963 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1963 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1963 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1963 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1963 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1964 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1964 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1964 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1964 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1964 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1964 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1964 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1964 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1964 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1964 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1965 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1965 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1965 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1965 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1965 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1965 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1965 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1965 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1965 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1965 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1966 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1966 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1966 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1966 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1966 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1966 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1966 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1966 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1966 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1966 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1967 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1967 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1967 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1967 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1967 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1967 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1967 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1967 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1967 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1967 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1968 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1968 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1968 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1968 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1967 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1968 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1968 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1968 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1968 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1968 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1969 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1969 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1969 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1969 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1969 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1969 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1969 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1969 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1969 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1969 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1970 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1970 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1970 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1970 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1970 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1970 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1970 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1970 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1970 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1970 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1971 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1971 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1971 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1971 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1971 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1971 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1971 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1971 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1971 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1971 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1972 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1972 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1972 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1972 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1972 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1972 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1972 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1972 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1972 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1972 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1973 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1973 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1973 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1973 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1973 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1973 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1973 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1973 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1973 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1973 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1974 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1974 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1974 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1974 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1974 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1974 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1974 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1974 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1974 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1974 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1975 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1975 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1975 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1975 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1975 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1975 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1975 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1975 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1975 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1975 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1976 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1976 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1976 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1976 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1976 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1976 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1976 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1976 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1976 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1976 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1977 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1977 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1977 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1977 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1977 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1977 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1977 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1977 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1977 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1977 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1978 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1978 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1978 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1978 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1977 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1978 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1978 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1978 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1978 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1978 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1979 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1979 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1979 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1979 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1979 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1979 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1979 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1979 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1979 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1979 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1980 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1980 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1980 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1980 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1980 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1980 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1980 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1980 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1980 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1980 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1981 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1981 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1981 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1981 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1981 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1981 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1981 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1981 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1981 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1981 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1982 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1982 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1982 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1982 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1982 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1982 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1982 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1982 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1982 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1982 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1983 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1983 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1983 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1983 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1983 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1983 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1983 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1983 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1983 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1983 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1984 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1984 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1984 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1984 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1984 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1984 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1984 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1984 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1984 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1984 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1985 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1985 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1985 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1985 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1985 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1985 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1985 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1985 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1985 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1985 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1986 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1986 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1986 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1986 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1986 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1986 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1986 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1986 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1986 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1986 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1987 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1987 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1987 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1987 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1987 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1987 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1987 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1987 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1987 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1987 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1988 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1988 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1988 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1988 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1987 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1988 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1988 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1988 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1988 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1988 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1989 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1989 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1989 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1989 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1989 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1989 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1989 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1989 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1989 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1989 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1990 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1990 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1990 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1990 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1990 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1990 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1990 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1990 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1990 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1990 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1991 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1991 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1991 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1991 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1991 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1991 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1991 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1991 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1991 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1991 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1992 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1992 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1992 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1992 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1992 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1992 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1992 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1992 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1992 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1992 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1993 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1993 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1993 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1993 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1993 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1993 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1993 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1993 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1993 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1993 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1994 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1994 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1994 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1994 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1994 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1994 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1994 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1994 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1994 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1994 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1995 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1995 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1995 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1995 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1995 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1995 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1995 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1995 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1995 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1995 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1996 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1996 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1996 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1996 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1996 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1996 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1996 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1996 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1996 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1996 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1997 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1997 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1997 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1997 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1997 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1997 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1997 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1997 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1997 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1997 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1998 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1998 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1998 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1998 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1997 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1998 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1998 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1998 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1998 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1998 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    1999 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR1999 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR1999 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR1999 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR1999 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR1999 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR1999 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR1999 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR1999 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR1999 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    2000 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR2000 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR2000 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR2000 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR2000 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR2000 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR2000 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR2000 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR2000 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR2000 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    2001 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR2001 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR2001 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR2001 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR2001 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR2001 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR2001 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR2001 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR2001 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR2001 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    2002 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR2002 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR2002 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR2002 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR2002 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR2002 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR2002 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR2002 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR2002 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR2002 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    2003 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR2003 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR2003 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR2003 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR2003 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR2003 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR2003 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR2003 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR2003 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR2003 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    2004 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR2004 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR2004 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR2004 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR2004 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR2004 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR2004 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR2004 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR2004 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR2004 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    2005 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR2005 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR2005 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR2005 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR2005 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR2005 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR2005 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR2005 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR2005 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR2005 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    2006 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR2006 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR2006 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR2006 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR2006 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR2006 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR2006 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR2006 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR2006 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR2006 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    2007 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR2007 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR2007 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR2007 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR2007 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR2007 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR2007 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR2007 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR2007 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR2007 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    2008 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR2008 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR2008 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR2008 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR2007 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR2008 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR2008 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR2008 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR2008 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR2008 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    2009 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR2009 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR2009 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR2009 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR2009 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR2009 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR2009 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR2009 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR2009 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR2009 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    2010 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR2010 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR2010 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR2010 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR2010 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR2010 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR2010 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR2010 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR2010 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR2010 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    2011 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR2011 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR2011 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR2011 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR2011 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR2011 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR2011 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR2011 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR2011 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR2011 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    2012 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR2012 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR2012 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR2012 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR2012 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR2012 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR2012 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR2012 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR2012 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR2012 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    2013 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR2013 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR2013 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR2013 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR2013 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR2013 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR2013 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR2013 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR2013 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR2013 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    2014 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR2014 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR2014 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR2014 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR2014 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR2014 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR2014 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR2014 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR2014 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR2014 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    2015 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR2015 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR2015 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR2015 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR2015 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR2015 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR2015 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR2015 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR2015 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR2015 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    2016 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR2016 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR2016 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR2016 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR2016 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR2016 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR2016 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR2016 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR2016 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR2016 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    2017 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR2017 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR2017 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR2017 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR2017 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR2017 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR2017 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR2017 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR2017 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR2017 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    2018 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR2018 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR2018 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR2018 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR2017 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR2018 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR2018 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR2018 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR2018 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR2018 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

union all

select Country_Name,
    2019 as Year,
    sum(case when Series_Name = 'Net official development assistance and official aid received (current US$)' then YR2019 else 0 end) as net_aid,
    sum(case when Series_Name = 'Foreign direct investment, net inflows (% of GDP)' then YR2019 else 0 end) as fdi_inflows,
    sum(case when Series_Name = 'Adjusted net national income (current US$)' then YR2019 else 0 end) as nni,
    sum(case when Series_Name = 'Inflation, GDP deflator (annual %)' then YR2019 else 0 end) as inflation,
    sum(case when Series_Name = 'Domestic credit provided by financial sector (% of GDP)' then YR2019 else 0 end) as credit,
    sum(case when Series_Name = 'Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)' then YR2019 else 0 end) as poverty_ratio,
    sum(case when Series_Name = 'Gross intake ratio in first grade of primary education, total (% of relevant age group)' then YR2019 else 0 end) as education,
    sum(case when Series_Name = 'Poverty gap at $1.90 a day (2011 PPP) (%)' then YR2019 else 0 end) as poverty_gap,
    sum(case when Series_Name = 'CPIA public sector management and institutions cluster average (1=low to 6=high)' then YR2019 else 0 end) as bureaucracy
from adsurgdb.africa
where Series_Name <> 'Series Name'
	and Country_Name <> ''
group by Country_Name

;



select distinct Series_Name
from adsurgdb.africa
where Series_Name not in ('Series Name','');
