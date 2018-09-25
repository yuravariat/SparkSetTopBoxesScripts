DECLARE @Abilene varchar(50),@LakeCharles varchar(50),@SeattleTacoma varchar(50)
SET @Abilene = 'Abilene-Sweetwater'
SET @LakeCharles = 'Lake Charles'
SET @SeattleTacoma = 'Seattle-Tacoma'

DECLARE @HouseHolds TABLE(
	HouseholdID varchar(100)
)

--Get Devices per city with one device in house
INSERT INTO @HouseHolds
SELECT d.HouseholdID FROM [dbo].[HouseHolders] as d
WHERE [HouseholdID] IN (SELECT p.HouseHoldID as c 
	FROM [dbo].[HouseHolders] as p
	WHERE p.Dma=@SeattleTacoma
	GROUP BY p.HouseHoldID
	HAVING COUNT(p.HouseholdID)=1
) 
AND d.PeriodStart=0 
AND d.DaysCount>90;

SELECT
	Household_ID,
	(case when Household_size<>'' then CAST(Household_size as int) else 0 end) as Household_size,
	(case when Number_of_Adults<>'' then CAST(Number_of_Adults as int) else 0 end) as Number_of_Adults,
	(case when Number_of_Generations_in_household<>'' then CAST(Number_of_Generations_in_household as int) else 0 end) as Number_of_Generations_in_household,
	(case when Marital_Status='M' then 1 
		when Marital_Status='S' then 2 
		when Marital_Status='A' then 3
		when Marital_Status='B' then 4 else 0 end) as Marital_Status,
	(case when Race_Code='A' then 1 
		when Race_Code='B' then 2 
		when Race_Code='H' then 3
		when Race_Code='W' then 4 else 0 end) as Race_Code,
	(case when Dwelling_Type='M' then 1 
		when Dwelling_Type='S' then 2 else 0 end) as Dwelling_Type,
	(case when Home_owner_status='O' then 1 
		when Home_owner_status='R' then 2 else 0 end) as Home_owner_status,
	(case when Length_of_Residence<>'' then CAST(Length_of_Residence as int) else 0 end) as Length_of_Residence,
	(case when Home_Market_Value='A' then 1 
		when Home_Market_Value='B' then 2
		when Home_Market_Value='C' then 3
		when Home_Market_Value='D' then 4
		when Home_Market_Value='E' then 5
		when Home_Market_Value='F' then 6
		when Home_Market_Value='G' then 7
		when Home_Market_Value='H' then 8
		when Home_Market_Value='I' then 9
		when Home_Market_Value='J' then 10
		when Home_Market_Value='K' then 11
		when Home_Market_Value='L' then 12
		when Home_Market_Value='M' then 13
		when Home_Market_Value='N' then 14
		when Home_Market_Value='O' then 15
		when Home_Market_Value='P' then 16
		when Home_Market_Value='Q' then 17
		when Home_Market_Value='R' then 18
		when Home_Market_Value='S' then 19 else 0 end) as Home_Market_Value,
	(case when Net_Worth<>'' then CAST(Net_Worth as int) else 0 end) as Net_Worth,
	(case when Income='A' then 10
		when Income='B' then 11
		when Income='C' then 12
		when Income='D' then 13 
		when Income<>'' then CAST(Income as int) else 0 end) as Income,
	(case when Gender_Individual='M' then 1 
		when Gender_Individual='F' then 2 else 0 end) as Gender_Individual,
	(case when Age_of_individuals<>'' then CAST(Age_of_individuals as int) else 0 end) as Age_of_individuals,
	(case when Education_Input_Individuals<>'' then CAST(Education_Input_Individuals as int) else 0 end) as Education_Input_Individuals,
	(case when Household_Clusters<>'' then CAST(SUBSTRING(Household_Clusters,0,3) as int) else 0 end) as Household_Clusters,
	(case when Insurance_Groups<>'' then CAST(SUBSTRING(Insurance_Groups,0,3) as int) else 0 end) as Insurance_Groups,
	(case when Financial_Groups<>'' then CAST(SUBSTRING(Financial_Groups,0,3) as int) else 0 end) as Financial_Groups,
	--Adult_ranges_present_in_household,
	(case when Adult_ranges_present_in_household<>'' then (case when SUBSTRING(Adult_ranges_present_in_household, 1, 1)='1' then 1 else 0 end) else 0 end) as [Males_18_-_24],
	(case when Adult_ranges_present_in_household<>'' then (case when SUBSTRING(Adult_ranges_present_in_household, 2, 1)='1' then 1 else 0 end) else 0 end) as [Females_18_-_24],
	(case when Adult_ranges_present_in_household<>'' then (case when SUBSTRING(Adult_ranges_present_in_household, 3, 1)='1' then 1 else 0 end) else 0 end) as [Unknown_Gender_18_-_24],
	(case when Adult_ranges_present_in_household<>'' then (case when SUBSTRING(Adult_ranges_present_in_household, 4, 1)='1' then 1 else 0 end) else 0 end) as [Males_25_-_34],
	(case when Adult_ranges_present_in_household<>'' then (case when SUBSTRING(Adult_ranges_present_in_household, 5, 1)='1' then 1 else 0 end) else 0 end) as [Females_25_-_34],
	(case when Adult_ranges_present_in_household<>'' then (case when SUBSTRING(Adult_ranges_present_in_household, 6, 1)='1' then 1 else 0 end) else 0 end) as [Unknown_Gender_25_-_34],
	(case when Adult_ranges_present_in_household<>'' then (case when SUBSTRING(Adult_ranges_present_in_household, 7, 1)='1' then 1 else 0 end) else 0 end) as [Males_35_-_44],
	(case when Adult_ranges_present_in_household<>'' then (case when SUBSTRING(Adult_ranges_present_in_household, 8, 1)='1' then 1 else 0 end) else 0 end) as [Females_35_-_44],
	(case when Adult_ranges_present_in_household<>'' then (case when SUBSTRING(Adult_ranges_present_in_household, 9, 1)='1' then 1 else 0 end) else 0 end) as [Unknown_Gender_35_-_44],
	(case when Adult_ranges_present_in_household<>'' then (case when SUBSTRING(Adult_ranges_present_in_household, 10, 1)='1' then 1 else 0 end) else 0 end) as [Males_45_-_54],
	(case when Adult_ranges_present_in_household<>'' then (case when SUBSTRING(Adult_ranges_present_in_household, 11, 1)='1' then 1 else 0 end) else 0 end) as [Females_45_-_54],
	(case when Adult_ranges_present_in_household<>'' then (case when SUBSTRING(Adult_ranges_present_in_household, 12, 1)='1' then 1 else 0 end) else 0 end) as [Unknown_Gender_45_-_54],
	(case when Adult_ranges_present_in_household<>'' then (case when SUBSTRING(Adult_ranges_present_in_household, 13, 1)='1' then 1 else 0 end) else 0 end) as [Males_55_-_64],
	(case when Adult_ranges_present_in_household<>'' then (case when SUBSTRING(Adult_ranges_present_in_household, 14, 1)='1' then 1 else 0 end) else 0 end) as [Females_55_-_64],
	(case when Adult_ranges_present_in_household<>'' then (case when SUBSTRING(Adult_ranges_present_in_household, 15, 1)='1' then 1 else 0 end) else 0 end) as [Unknown_Gender_55_-_64],
	(case when Adult_ranges_present_in_household<>'' then (case when SUBSTRING(Adult_ranges_present_in_household, 16, 1)='1' then 1 else 0 end) else 0 end) as [Males_65_-_74],
	(case when Adult_ranges_present_in_household<>'' then (case when SUBSTRING(Adult_ranges_present_in_household, 17, 1)='1' then 1 else 0 end) else 0 end) as [Females_65_-_74],
	(case when Adult_ranges_present_in_household<>'' then (case when SUBSTRING(Adult_ranges_present_in_household, 18, 1)='1' then 1 else 0 end) else 0 end) as [Unknown_Gender_65_-_74],
	(case when Adult_ranges_present_in_household<>'' then (case when SUBSTRING(Adult_ranges_present_in_household, 19, 1)='1' then 1 else 0 end) else 0 end) as [Males_75+],
	(case when Adult_ranges_present_in_household<>'' then (case when SUBSTRING(Adult_ranges_present_in_household, 20, 1)='1' then 1 else 0 end) else 0 end) as [Females_75+],
	(case when Adult_ranges_present_in_household<>'' then (case when SUBSTRING(Adult_ranges_present_in_household, 21, 1)='1' then 1 else 0 end) else 0 end) as [Unknown_Gender_75+]
FROM Demodata
WHERE Household_ID IN (SELECT HouseholdID FROM @HouseHolds)