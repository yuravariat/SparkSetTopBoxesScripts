USE [SintecMedia]
GO


/*
	Slice demodata
*/
BEGIN

DECLARE @DemodataSlice TABLE (
	[Household_ID] [varchar](50) NULL,
	[Household_size] [varchar](50) NULL,
	[Number_of_Adults] [varchar](50) NULL,
	[Number_of_Generations_in_household] [varchar](50) NULL,
	[Adult_ranges_present_in_household] [varchar](50) NULL,
	[Marital_Status] [varchar](50) NULL,
	[Race_Code] [varchar](50) NULL,
	[Presence_of_Children] [varchar](50) NULL,
	[Number_of_Children] [varchar](50) NULL,
	[Childrens_Ages_Age] [varchar](50) NULL,
	[Childrens_age_ranges] [varchar](50) NULL,
	[Dwelling_Type] [varchar](50) NULL,
	[Home_owner_status] [varchar](50) NULL,
	[Length_of_Residence] [varchar](50) NULL,
	[Home_Market_Value] [varchar](50) NULL,
	[Number_of_Vehicles] [varchar](50) NULL,
	[Vehicle_Make_Code] [varchar](50) NULL,
	[Vehicle_Model_Code] [varchar](50) NULL,
	[Vehicle_Year] [varchar](50) NULL,
	[Net_Worth] [varchar](50) NULL,
	[Income] [varchar](50) NULL,
	[Gender_Individual] [varchar](50) NULL,
	[Age_of_individuals] [varchar](50) NULL,
	[Education_Input_Individuals] [varchar](50) NULL,
	[Occupation_Input_Individuals] [varchar](50) NULL,
	[Education_of_1st_Individual] [varchar](50) NULL,
	[Occupation_1st_Individual] [varchar](50) NULL,
	[Age_of_the_2nd_individual] [varchar](50) NULL,
	[Education_of_2nd_Individual] [varchar](50) NULL,
	[Occupation_2nd_Individual] [varchar](50) NULL,
	[Age_of_the_3rd_individual] [varchar](50) NULL,
	[Education_of_3rd_Individual] [varchar](50) NULL,
	[Occupation_3rd_Individual] [varchar](50) NULL,
	[Age_of_4th_Individual] [varchar](50) NULL,
	[Education_of_4th_Individual] [varchar](50) NULL,
	[Occupation_4th_Individual] [varchar](50) NULL,
	[Age_of_5th_Individual] [varchar](50) NULL,
	[Education_of_5th_Individual] [varchar](50) NULL,
	[Occupation_5th_Individual] [varchar](50) NULL,
	[Political_Party_1st_Individual] [varchar](50) NULL,
	[Voter_Party] [varchar](50) NULL,
	[Household_Clusters] [varchar](50) NULL,
	[Insurance_Groups] [varchar](50) NULL,
	[Financial_Groups] [varchar](50) NULL,
	[Green_Living] [varchar](50) NULL
)

INSERT INTO @DemodataSlice
SELECT * FROM Demodata 
WHERE Household_ID IN (
	SELECT HouseholdID FROM HouseHolders
	WHERE (Mso + '-' + DeviceID) IN 
		(SELECT device_id FROM For_Demo_Analysis 
		WHERE total_parts=1 AND use_time_decay=0 AND cluster_number=1 AND area='Amarillo')
)

END

BEGIN

DECLARE @DemoSliceTotal FLOAT
SET @DemoSliceTotal = (SELECT COUNT(0) FROM @DemodataSlice)

--Count of @DemodataSlice fields
DECLARE @DemodataSliceFieldsTable TABLE(
	[Information type] nchar(50) NULL,
	[Available data] FLOAT NULL
)
INSERT INTO @DemodataSliceFieldsTable VALUES ('Household_size',(select count(0)/@DemoSliceTotal from @DemodataSlice where Household_size<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Number_of_Adults',(select count(0)/@DemoSliceTotal from @DemodataSlice where Number_of_Adults<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Number_of_Generations_in_household',(select count(0)/@DemoSliceTotal from @DemodataSlice where Number_of_Generations_in_household<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Adult_ranges_present_in_household',(select count(0)/@DemoSliceTotal from @DemodataSlice where Adult_ranges_present_in_household<>'' and Adult_ranges_present_in_household<>'000000000000000000000'))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Marital_Status',(select count(0)/@DemoSliceTotal from @DemodataSlice where Marital_Status<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Race_Code',(select count(0)/@DemoSliceTotal from @DemodataSlice where Race_Code<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Presence_of_Children',(select count(0)/@DemoSliceTotal from @DemodataSlice where Presence_of_Children<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Number_of_Children',(select count(0)/@DemoSliceTotal from @DemodataSlice where Number_of_Children<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Childrens_Ages_Age',(select count(0)/@DemoSliceTotal from @DemodataSlice where Childrens_Ages_Age<>'' and Childrens_Ages_Age<>'0000000000000000000'))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Childrens_age_ranges',(select count(0)/@DemoSliceTotal from @DemodataSlice where Childrens_age_ranges<>'' and Childrens_age_ranges<>'000000000000000'))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Dwelling_Type',(select count(0)/@DemoSliceTotal from @DemodataSlice where Dwelling_Type<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Home_owner_status',(select count(0)/@DemoSliceTotal from @DemodataSlice where Home_owner_status<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Length_of_Residence',(select count(0)/@DemoSliceTotal from @DemodataSlice where Length_of_Residence<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Home_Market_Value',(select count(0)/@DemoSliceTotal from @DemodataSlice where Home_Market_Value<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Number_of_Vehicles',(select count(0)/@DemoSliceTotal from @DemodataSlice where Number_of_Vehicles<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Vehicle_Make_Code',(select count(0)/@DemoSliceTotal from @DemodataSlice where Vehicle_Make_Code<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Vehicle_Model_Code',(select count(0)/@DemoSliceTotal from @DemodataSlice where Vehicle_Model_Code<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Vehicle_Year',(select count(0)/@DemoSliceTotal from @DemodataSlice where Vehicle_Year<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Net_Worth',(select count(0)/@DemoSliceTotal from @DemodataSlice where Net_Worth<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Income',(select count(0)/@DemoSliceTotal from @DemodataSlice where Income<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Gender_Individual',(select count(0)/@DemoSliceTotal from @DemodataSlice where Gender_Individual<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Age_of_individuals',(select count(0)/@DemoSliceTotal from @DemodataSlice where Age_of_individuals<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Education_Input_Individuals',(select count(0)/@DemoSliceTotal from @DemodataSlice where Education_Input_Individuals<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Occupation_Input_Individuals',(select count(0)/@DemoSliceTotal from @DemodataSlice where Occupation_Input_Individuals<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Education_of_1st_Individual',(select count(0)/@DemoSliceTotal from @DemodataSlice where Education_of_1st_Individual<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Occupation_1st_Individual',(select count(0)/@DemoSliceTotal from @DemodataSlice where Occupation_1st_Individual<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Age_of_the_2nd_individual',(select count(0)/@DemoSliceTotal from @DemodataSlice where Age_of_the_2nd_individual<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Education_of_2nd_Individual',(select count(0)/@DemoSliceTotal from @DemodataSlice where Education_of_2nd_Individual<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Occupation_2nd_Individual',(select count(0)/@DemoSliceTotal from @DemodataSlice where Occupation_2nd_Individual<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Age_of_the_3rd_individual',(select count(0)/@DemoSliceTotal from @DemodataSlice where Age_of_the_3rd_individual<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Education_of_3rd_Individual',(select count(0)/@DemoSliceTotal from @DemodataSlice where Education_of_3rd_Individual<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Occupation_3rd_Individual',(select count(0)/@DemoSliceTotal from @DemodataSlice where Occupation_3rd_Individual<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Age_of_4th_Individual',(select count(0)/@DemoSliceTotal from @DemodataSlice where Age_of_4th_Individual<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Education_of_4th_Individual',(select count(0)/@DemoSliceTotal from @DemodataSlice where Education_of_4th_Individual<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Occupation_4th_Individual',(select count(0)/@DemoSliceTotal from @DemodataSlice where Occupation_4th_Individual<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Age_of_5th_Individual',(select count(0)/@DemoSliceTotal from @DemodataSlice where Age_of_5th_Individual<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Education_of_5th_Individual',(select count(0)/@DemoSliceTotal from @DemodataSlice where Education_of_5th_Individual<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Occupation_5th_Individual',(select count(0)/@DemoSliceTotal from @DemodataSlice where Occupation_5th_Individual<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Political_Party_1st_Individual',(select count(0)/@DemoSliceTotal from @DemodataSlice where Political_Party_1st_Individual<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Voter_Party',(select count(0)/@DemoSliceTotal from @DemodataSlice where Voter_Party<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Household_Clusters',(select count(0)/@DemoSliceTotal from @DemodataSlice where Household_Clusters<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Insurance_Groups',(select count(0)/@DemoSliceTotal from @DemodataSlice where Insurance_Groups<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Financial_Groups',(select count(0)/@DemoSliceTotal from @DemodataSlice where Financial_Groups<>''))
INSERT INTO @DemodataSliceFieldsTable VALUES ('Green_Living',(select count(0)/@DemoSliceTotal from @DemodataSlice where Green_Living<>''))

SELECT * FROM @DemodataSliceFieldsTable

--Households by size

SELECT (case when Household_size='' then 'Unknown' else Household_size end) as [Persons in household],count(0) as [Households] 
FROM @DemodataSlice
GROUP BY Household_size
ORDER BY [Persons in household]

--Households by Number_of_Adults
SELECT (case when Number_of_Adults='' then 'Unknown' else Number_of_Adults end) as [Adults in household],count(0) as [Households] 
FROM @DemodataSlice
GROUP BY Number_of_Adults
ORDER BY [Adults in household]

--Households by Presence_of_Children
select (case when Presence_of_Children='' then 'Unknown' else Presence_of_Children end) as [Presence of Children],count(0) as [Households] from @DemodataSlice
group by Presence_of_Children

--Households by Number_of_Children
select (case when Number_of_Children='' then 'Unknown' else Number_of_Children end) 
as [Number of Children],count(0) as [Households] from @DemodataSlice
group by Number_of_Children
ORDER BY [Number of Children]

--Households by Marital_Status
SELECT (case when Marital_Status='M' then 'Married' 
		when Marital_Status='S' then 'Single' 
		when Marital_Status='A' then 'Inferred Married'
		when Marital_Status='B' then 'Inferred Single' else 'Unknown' end)  
as [Marital status],count(0) as [Households] from @DemodataSlice
GROUP BY Marital_Status
ORDER BY [Marital status]

--Households by Number_of_Generations_in_household
SELECT (case when Number_of_Generations_in_household='' then 'Unknown' else Number_of_Generations_in_household end) as [Number of Generations in household],count(0) as [Households] 
FROM @DemodataSlice
GROUP BY Number_of_Generations_in_household
ORDER BY [Number of Generations in household]

--Households by Race_Code
SELECT (case WHEN Race_Code='A' THEN 'Asian' 
		WHEN Race_Code='B' THEN 'African American' 
		WHEN Race_Code='H' THEN 'Hispanic'
		WHEN Race_Code='W' THEN 'White/Other' ELSE 'Unknown' END) 
as [Race in household],count(0) as [Households] from @DemodataSlice
GROUP BY Race_Code
ORDER BY [Race in household]

--Households by Dwelling_Type
SELECT (case when Dwelling_Type='M' then 'Multiple Family Dwelling Unit' 
		when Dwelling_Type='S' then 'Single Family Dwelling Unit' else 'Unknown' end) 
as [Dwelling type],count(0) as [Households] from @DemodataSlice
GROUP BY Dwelling_Type
ORDER BY [Dwelling type]

--Households by Home_owner_status
SELECT (case when Home_owner_status='O' then 'Home Owner' 
		when Home_owner_status='R' then 'Renter' else 'Unknown' end)  
as [Home owner status],count(0) as [Households] from @DemodataSlice
GROUP BY Home_owner_status
ORDER BY [Home owner status]

--Households by Length_of_Residence
SELECT (case when Length_of_Residence='' then 'Unknown' else Length_of_Residence end) as [Length of Residence],count(0) as [Households] from @DemodataSlice
GROUP BY Length_of_Residence
ORDER BY [Length of Residence]

--Households by Home_Market_Value
SELECT (case when Home_Market_Value='A' then '$1,000-$24,999' 
		when Home_Market_Value='B' then '$25,000-$49,999' 
		when Home_Market_Value='C' then '$50,000-$74,999'
		when Home_Market_Value='D' then '$75,000-$99,999'
		when Home_Market_Value='E' then '$100,000-$124,999'
		when Home_Market_Value='F' then '$125,000-$149,999'
		when Home_Market_Value='G' then '$150,000-$174,999'
		when Home_Market_Value='H' then '$175,000-$199,999'
		when Home_Market_Value='I' then '$200,000-$224,999'
		when Home_Market_Value='J' then '$225,000-$249,999'
		when Home_Market_Value='K' then '$250,000-$274,000'
		when Home_Market_Value='L' then '$275,000-$299,999'
		when Home_Market_Value='M' then '$300,000-$349,999'
		when Home_Market_Value='N' then '$350,000-$399,999'
		when Home_Market_Value='O' then '$400,000-$449,999' 
		when Home_Market_Value='P' then '$450,000-$499,999'
		when Home_Market_Value='Q' then '$500,000-$749,999'
		when Home_Market_Value='R' then '$750,000-$999,999'
		when Home_Market_Value='S' then '$1,000,000 Plus' else 'Unknown' end)
as [Home Market Value],count(0) as [Households] from @DemodataSlice
GROUP by Home_Market_Value

--Households by Net_Worth
select (case when Net_Worth='1' then 'Less than $1' 
		when Net_Worth='2' then '$1-$4,999' 
		when Net_Worth='3' then '$5,00-$9,999'
		when Net_Worth='4' then '$10,000-$24,999'
		when Net_Worth='5' then '$25,000-$49,999'
		when Net_Worth='6' then '$50,000-$99,999'
		when Net_Worth='7' then '$100,000-$249,000'
		when Net_Worth='8' then '$250,000-$499,999'
		when Net_Worth='9' then 'Greater than $499,999' else 'Unknown' end)
as [Net Worth],count(0) as [Households] from @DemodataSlice
group by Net_Worth

--Households by Income
SELECT (case when Income ='1' then 'Less than $15,000' 
		when Income='2' then '$15,000-$19,999' 
		when Income='3' then '$20,000-$29,999'
		when Income='4' then '$30,000-$39,999'
		when Income='5' then '$40,000-$49,999'
		when Income='6' then '$50,000-$59,999'
		when Income='7' then '$60,000-$69,999'
		when Income='8' then '$70,000-$79,999'
		when Income='9' then '$80,000-$89,999'
		when Income='A' then '$90,000-$99,999'
		when Income='B' then '$100,000-$124,999'
		when Income='C' then '$125,000-$149,999'
		when Income='D' then 'Greater than $149,999' else 'Unknown' end)
as [Income],count(0) as [Households] from @DemodataSlice
GROUP BY Income

--Households by Gender_Individual
select (case when Gender_Individual='M' then 'Male' 
		when Gender_Individual='F' then 'Female' else 'Unknown' end) 
as [Gender],count(0) as [Households] from @DemodataSlice
group by Gender_Individual
ORDER BY [Gender]

--Households by Age_of_individuals
select (case when Age_of_individuals='17' then 'Age is less than 18'
		when Age_of_individuals='18' then 'Age 18-19'
		when Age_of_individuals='20' then 'Age 20-21'
		when Age_of_individuals='22' then 'Age 22-23'
		when Age_of_individuals='24' then 'Age 24-25'
		when Age_of_individuals='26' then 'Age 26-27'
		when Age_of_individuals='28' then 'Age 28-29'
		when Age_of_individuals='30' then 'Age 30-31'
		when Age_of_individuals='32' then 'Age 32-33'
		when Age_of_individuals='34' then 'Age 34-35'
		when Age_of_individuals='36' then 'Age 36-37'
		when Age_of_individuals='38' then 'Age 38-39'
		when Age_of_individuals='40' then 'Age 40-41'
		when Age_of_individuals='42' then 'Age 42-43'
		when Age_of_individuals='44' then 'Age 44-45'
		when Age_of_individuals='46' then 'Age 46-47'
		when Age_of_individuals='48' then 'Age 48-49'
		when Age_of_individuals='50' then 'Age 50-51'
		when Age_of_individuals='52' then 'Age 52-53'
		when Age_of_individuals='54' then 'Age 54-55'
		when Age_of_individuals='56' then 'Age 56-57'
		when Age_of_individuals='58' then 'Age 58-59'
		when Age_of_individuals='60' then 'Age 59-60'
		when Age_of_individuals='62' then 'Age 62-63'
		when Age_of_individuals='64' then 'Age 64-65'
		when Age_of_individuals='66' then 'Age 66-67'
		when Age_of_individuals='68' then 'Age 68-69'
		when Age_of_individuals='70' then 'Age 70-71'
		when Age_of_individuals='72' then 'Age 72-73'
		when Age_of_individuals='74' then 'Age 74-75'
		when Age_of_individuals='76' then 'Age 76-77'
		when Age_of_individuals='78' then 'Age 78-79'
		when Age_of_individuals='80' then 'Age 80-81'
		when Age_of_individuals='82' then 'Age 82-83'
		when Age_of_individuals='84' then 'Age 84-85'
		when Age_of_individuals='86' then 'Age 86-87'
		when Age_of_individuals='88' then 'Age 88-89'
		when Age_of_individuals='90' then 'Age 90-91'
		when Age_of_individuals='92' then 'Age 92-93'
		when Age_of_individuals='94' then 'Age 94-95'
		when Age_of_individuals='96' then 'Age 96-97'
		when Age_of_individuals='98' then 'Age 98-99'
		when Age_of_individuals='99' then 'Age greater than 99' else 'Unknown' end)
as [Age of individuals],count(0) as [Households] from @DemodataSlice
group by Age_of_individuals
ORDER BY [Age of individuals]

--Households by Education_Input_Individuals
select (case when Education_Input_Individuals='1' then 'Completed High School' 
		when Education_Input_Individuals='2' then 'Completed College' 
		when Education_Input_Individuals='3' then 'Completed Graduate School'
		when Education_Input_Individuals='4' then 'Attended Vocational/Technical' else 'Unknown' end)
as [Education Individuals],count(0) as [Households] from @DemodataSlice
group by Education_Input_Individuals
ORDER BY [Education Individuals]

--Households by Household_Clusters
select ClusterName, SUM(Households) as Households from 
(select Household_Clusters,(case when Household_Clusters like '01%' then 'Summit Estates'
		when Household_Clusters like '02%' then 'Established Elite'
		when Household_Clusters like '03%' then 'Corporate Clout'
		when Household_Clusters like '04%' then 'Skyboxes and Suburbans'
		when Household_Clusters like '05%' then 'Sitting Pretty'
		when Household_Clusters like '06%' then 'Shooting Stars'
		when Household_Clusters like '07%' then 'Lavish Lifestyles'
		when Household_Clusters like '08%' then 'Full Steaming'
		when Household_Clusters like '09%' then 'Platinum Oldies'
		when Household_Clusters like '10%' then 'Hard Chargers'
		when Household_Clusters like '11%' then 'Kids and Clout'
		when Household_Clusters like '12%' then 'Tots and Toys'
		when Household_Clusters like '13%' then 'Solid Single Parents'
		when Household_Clusters like '14%' then 'Career-Centered Singles'
		when Household_Clusters like '15%' then 'Country Ways'
		when Household_Clusters like '16%' then 'Country Single'
		when Household_Clusters like '17%' then 'Apple Pie Families'
		when Household_Clusters like '18%' then 'Married Sophisticated'
		when Household_Clusters like '19%' then 'Country Comfort'
		when Household_Clusters like '20%' then 'Dynamic Duos'
		when Household_Clusters like '21%' then 'Children First'
		when Household_Clusters like '22%' then 'Fun and Games'
		when Household_Clusters like '23%' then 'Acres Couples'
		when Household_Clusters like '24%' then 'Career Building'
		when Household_Clusters like '25%' then 'Clubs and Causes'
		when Household_Clusters like '26%' then 'Savvy Singles'
		when Household_Clusters like '27%' then 'Soccer and SUVs'
		when Household_Clusters like '28%' then 'Suburban Seniors'
		when Household_Clusters like '29%' then 'City Mixers'
		when Household_Clusters like '30%' then 'Spouses and Houses'
		when Household_Clusters like '31%' then 'Mid Americana'
		when Household_Clusters like '32%' then 'Metro Mix'
		when Household_Clusters like '33%' then 'Urban Tenants'
		when Household_Clusters like '34%' then 'Outward Bound'
		when Household_Clusters like '35%' then 'Solo and Stable'
		when Household_Clusters like '36%' then 'Raisin’ Grandkids'
		when Household_Clusters like '37%' then 'Cartoons and Carpools'
		when Household_Clusters like '38%' then 'Midtown Minivanners'
		when Household_Clusters like '39%' then 'Early Parents'
		when Household_Clusters like '40%' then 'The Great Outdoors'
		when Household_Clusters like '41%' then 'Truckin’ & Stylin’'
		when Household_Clusters like '42%' then 'First Mortgage'
		when Household_Clusters like '43%' then 'Work & Causes'
		when Household_Clusters like '44%' then 'Community Singles'
		when Household_Clusters like '45%' then 'First Digs'
		when Household_Clusters like '46%' then 'Home Cooking'
		when Household_Clusters like '47%' then 'Rural Parents'
		when Household_Clusters like '48%' then 'Farmland Families'
		when Household_Clusters like '49%' then 'Devoted Duos'
		when Household_Clusters like '50%' then 'Rural Retirement'
		when Household_Clusters like '51%' then 'Family Matters'
		when Household_Clusters like '52%' then 'Resolution Renters'
		when Household_Clusters like '53%' then 'Metro Parents'
		when Household_Clusters like '54%' then 'Still Truckin’'
		when Household_Clusters like '55%' then 'Humble Homes'
		when Household_Clusters like '56%' then 'Modest Wages'
		when Household_Clusters like '57%' then 'Collegiate Crowd'
		when Household_Clusters like '58%' then 'Young Workboots'
		when Household_Clusters like '59%' then 'Mobile Mixers'
		when Household_Clusters like '60%' then 'Rural Rovers'
		when Household_Clusters like '61%' then 'Urban Scramble'
		when Household_Clusters like '62%' then 'Kids and Rent'
		when Household_Clusters like '63%' then 'Downtown Dwellers'
		when Household_Clusters like '64%' then 'Rural Everlasting'
		when Household_Clusters like '65%' then 'Thrifty Elders'
		when Household_Clusters like '66%' then 'Timeless Elders'
		when Household_Clusters like '67%' then 'Rolling Stones'
		when Household_Clusters like '68%' then 'Pennywise Proprietors'
		when Household_Clusters like '69%' then 'Pennywise Mortgages'
		when Household_Clusters like '70%' then 'Resilient Renters' else 'Unknown' end)
as ClusterName,count(0) as [Households] from @DemodataSlice
GROUP BY Household_Clusters) as t
GROUP BY t.ClusterName
ORDER BY t.ClusterName

--Households by Insurance_Groups
select [Insurance_Group] as [Insurance Group], SUM(Households) as Households from 
(select Insurance_Groups,(case when Insurance_Groups like '01%' then 'Secured Prosperity'
	when Insurance_Groups like '02%' then 'Stable Singles'
	when Insurance_Groups like '03%' then 'Single Opportunities'
	when Insurance_Groups like '04%' then 'Pennywise Homeowners'
	when Insurance_Groups like '05%' then 'Parenting Priorities'
	when Insurance_Groups like '06%' then 'Comfortable Renters'
	when Insurance_Groups like '07%' then 'Retirement Requirements'
	when Insurance_Groups like '08%' then 'Prosperous Families'
	when Insurance_Groups like '09%' then 'Comfortable Empty'
	when Insurance_Groups like '10%' then 'Modest Maturity'
	when Insurance_Groups like '11%' then 'Modest Country'
	when Insurance_Groups like '12%' then 'Aspiring Affluence'
	when Insurance_Groups like '13%' then 'Pennywise Renters' else 'Unknown' end)
as [Insurance_Group],count(0) as [Households] from @DemodataSlice
GROUP BY Insurance_Groups) as t
GROUP BY [Insurance_Group]
ORDER BY [Insurance_Group]

--Households by Financial_Groups
select t1.[Financial_Groups] as [Financial Group], SUM(Households) as Households from 
(select (case when Financial_Groups like '01%' then 'Urban Investors'
	when Financial_Groups like '02%' then 'Suburban Investors'
	when Financial_Groups like '03%' then 'Cautious Planners'
	when Financial_Groups like '04%' then 'Safety First'
	when Financial_Groups like '05%' then 'Savvy Investors'
	when Financial_Groups like '06%' then 'Country Caution'
	when Financial_Groups like '07%' then 'New Market Singles'
	when Financial_Groups like '08%' then 'Cash and Carry Urbanites'
	when Financial_Groups like '09%' then 'Cash and Carry Suburbanites'
	when Financial_Groups like '10%' then 'Rural Security '
	when Financial_Groups like '11%' then 'Getting Started'
	when Financial_Groups like '12%' then 'Financially Challenged ' else 'Unknown' end)
as Financial_Groups,count(0) as [Households] from @DemodataSlice
group by Financial_Groups) as t1
group by t1.Financial_Groups
ORDER BY Financial_Groups

--Ages renges of adults
DECLARE @ADULTS_AGE_RANGES TABLE (
	Household_ID nvarchar(50),
	DeviceID nvarchar(50),
	[Males_18_-_24] tinyint,
	[Females_18_-_24] tinyint,
	[Unknown_Gender_18_-_24] tinyint,
	[Males_25_-_34] tinyint,
	[Females_25_-_34] tinyint,
	[Unknown_Gender_25_-_34] tinyint,
	[Males_35_-_44] tinyint,
	[Females_35_-_44] tinyint,
	[Unknown_Gender_35_-_44] tinyint,
	[Males_45_-_54] tinyint,
	[Females_45_-_54] tinyint,
	[Unknown_Gender_45_-_54] tinyint,
	[Males_55_-_64] tinyint,
	[Females_55_-_64] tinyint,
	[Unknown_Gender_55_-_64] tinyint,
	[Males_65_-_74] tinyint,
	[Females_65_-_74] tinyint,
	[Unknown_Gender_65_-_74] tinyint,
	[Males_75+] tinyint,
	[Females_75+] tinyint,
	[Unknown_Gender_75+] tinyint)

DECLARE @HouseholdID nvarchar(50)
DECLARE @Ages nvarchar(50)
DECLARE @SplitTable TABLE (
	Num int,
    Item NVARCHAR(1000)
)
DECLARE Employee_Cursor CURSOR FOR  
SELECT Household_ID, Adult_ranges_present_in_household FROM @DemodataSlice
WHERE Adult_ranges_present_in_household<>'' 
	AND Adult_ranges_present_in_household<>'000000000000000000000'
	AND Adult_ranges_present_in_household<>'00000000000000000000A'
	AND CHARINDEX('G', Adult_ranges_present_in_household) = 0
OPEN Employee_Cursor;  
FETCH NEXT FROM Employee_Cursor into @HouseholdID,@Ages;  
WHILE @@FETCH_STATUS = 0  
   BEGIN
	  INSERT INTO @SplitTable
	  SELECT * FROM SplitString(SUBSTRING(@Ages,1,21),NULL)

	  INSERT INTO @ADULTS_AGE_RANGES
	  SELECT @HouseholdID,
	  '',
	  (select item from @SplitTable where Num=1),
	  (select item from @SplitTable where Num=2),
	  (select item from @SplitTable where Num=3),
	  (select item from @SplitTable where Num=4),
	  (select item from @SplitTable where Num=5),
	  (select item from @SplitTable where Num=6),
	  (select item from @SplitTable where Num=7),
	  (select item from @SplitTable where Num=8),
	  (select item from @SplitTable where Num=9),
	  (select item from @SplitTable where Num=10),
	  (select item from @SplitTable where Num=11),
	  (select item from @SplitTable where Num=12),
	  (select item from @SplitTable where Num=13),
	  (select item from @SplitTable where Num=14),
	  (select item from @SplitTable where Num=15),
	  (select item from @SplitTable where Num=16),
	  (select item from @SplitTable where Num=17),
	  (select item from @SplitTable where Num=18),
	  (select item from @SplitTable where Num=19),
	  (select item from @SplitTable where Num=20),
	  (select item from @SplitTable where Num=21)

      FETCH NEXT FROM Employee_Cursor into @HouseholdID,@Ages
	  DELETE FROM @SplitTable
   END;  
CLOSE Employee_Cursor;  
DEALLOCATE Employee_Cursor;  

select 
    SUM([Males_18_-_24]) as [Males_18_-_24],
	SUM([Females_18_-_24])as [Females_18_-_24],
	SUM([Unknown_Gender_18_-_24]) as [Unknown_Gender_18_-_24],
	SUM([Males_25_-_34]) as [Males_25_-_34],
	SUM([Females_25_-_34]) as [Females_25_-_34],
	SUM([Unknown_Gender_25_-_34]) as [Unknown_Gender_25_-_34],
	SUM([Males_35_-_44]) as [Males_35_-_44],
	SUM([Females_35_-_44]) as [Females_35_-_44],
	SUM([Unknown_Gender_35_-_44]) as [Unknown_Gender_35_-_44],
	SUM([Males_45_-_54]) as [Males_45_-_54],
	SUM([Females_45_-_54]) as [Females_45_-_54],
	SUM([Unknown_Gender_45_-_54]) as [Unknown_Gender_45_-_54],
	SUM([Males_55_-_64]) as [Males_55_-_64],
	SUM([Females_55_-_64]) as [Females_55_-_64],
	SUM([Unknown_Gender_55_-_64]) as [Unknown_Gender_55_-_64],
	SUM([Males_65_-_74]) as [Males_65_-_74],
	SUM([Females_65_-_74]) as [Females_65_-_74],
	SUM([Unknown_Gender_65_-_74]) as [Unknown_Gender_65_-_74],
	SUM([Males_75+]) as [Males_75+],
	SUM([Females_75+]) as [Females_75+],
	SUM([Unknown_Gender_75+]) as [Unknown_Gender_75+]
from @ADULTS_AGE_RANGES

END