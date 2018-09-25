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
	WHERE p.Dma=@Abilene
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
	(case when Marital_Status='M' then 1 else 0 end) as Marital_Status_M,
	(case when Marital_Status='S' then 1 else 0 end) as Marital_Status_S,
	(case when Marital_Status='A' then 1 else 0 end) as Marital_Status_A,
	(case when Marital_Status='B' then 1 else 0 end) as Marital_Status_B,
	(case when Race_Code='A' then 1 else 0 end) as Race_Code_A,
	(case when Race_Code='B' then 1 else 0 end) as Race_Code_B,
	(case when Race_Code='H' then 1 else 0 end) as Race_Code_H,
	(case when Race_Code='W' then 1 else 0 end) as Race_Code_W,
	(case when Dwelling_Type='M' then 1 else 0 end) as Dwelling_Type_M,
	(case when Dwelling_Type='S' then 1 else 0 end) as Dwelling_Type_S,
	(case when Home_owner_status='O' then 1 else 0 end) as Home_owner_status_O, 
	(case when Home_owner_status='R' then 1 else 0 end) as Home_owner_status_R,
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
	(case when Gender_Individual='M' then 1 else 0 end) as Gender_Individual_M,
	(case when Gender_Individual='F' then 1 else 0 end) as Gender_Individual_F,
	(case when Age_of_individuals<>'' then CAST(Age_of_individuals as int) else 0 end) as Age_of_individuals,
	(case when Education_Input_Individuals='1' then 1 else 0 end) as Education_Input_Individuals_High_School,
	(case when Education_Input_Individuals='2' then 1 else 0 end) as Education_Input_Individuals_College,
	(case when Education_Input_Individuals='3' then 1 else 0 end) as Education_Input_Individuals_Graduate_School,
	(case when Education_Input_Individuals='4' then 1 else 0 end) as Education_Input_Individuals_Technical,
	--(case when Household_Clusters<>'' then CAST(SUBSTRING(Household_Clusters,0,3) as int) else 0 end) as Household_Clusters,
	(case when Household_Clusters like '01%' then 1 else 0 end) as Household_Clusters_01,
	(case when Household_Clusters like '02%' then 1 else 0 end) as Household_Clusters_02,
	(case when Household_Clusters like '03%' then 1 else 0 end) as Household_Clusters_03,
	(case when Household_Clusters like '04%' then 1 else 0 end) as Household_Clusters_04,
	(case when Household_Clusters like '05%' then 1 else 0 end) as Household_Clusters_05,
	(case when Household_Clusters like '06%' then 1 else 0 end) as Household_Clusters_06,
	(case when Household_Clusters like '07%' then 1 else 0 end) as Household_Clusters_07,
	(case when Household_Clusters like '08%' then 1 else 0 end) as Household_Clusters_08,
	(case when Household_Clusters like '09%' then 1 else 0 end) as Household_Clusters_09,
	(case when Household_Clusters like '10%' then 1 else 0 end) as Household_Clusters_10,
	(case when Household_Clusters like '11%' then 1 else 0 end) as Household_Clusters_11,
	(case when Household_Clusters like '12%' then 1 else 0 end) as Household_Clusters_12,
	(case when Household_Clusters like '13%' then 1 else 0 end) as Household_Clusters_13,
	(case when Household_Clusters like '14%' then 1 else 0 end) as Household_Clusters_14,
	(case when Household_Clusters like '15%' then 1 else 0 end) as Household_Clusters_15,
	(case when Household_Clusters like '16%' then 1 else 0 end) as Household_Clusters_16,
	(case when Household_Clusters like '17%' then 1 else 0 end) as Household_Clusters_17,
	(case when Household_Clusters like '18%' then 1 else 0 end) as Household_Clusters_18,
	(case when Household_Clusters like '19%' then 1 else 0 end) as Household_Clusters_19,
	(case when Household_Clusters like '20%' then 1 else 0 end) as Household_Clusters_20,
	(case when Household_Clusters like '21%' then 1 else 0 end) as Household_Clusters_21,
	(case when Household_Clusters like '22%' then 1 else 0 end) as Household_Clusters_22,
	(case when Household_Clusters like '23%' then 1 else 0 end) as Household_Clusters_23,
	(case when Household_Clusters like '24%' then 1 else 0 end) as Household_Clusters_24,
	(case when Household_Clusters like '25%' then 1 else 0 end) as Household_Clusters_25,
	(case when Household_Clusters like '26%' then 1 else 0 end) as Household_Clusters_26,
	(case when Household_Clusters like '27%' then 1 else 0 end) as Household_Clusters_27,
	(case when Household_Clusters like '28%' then 1 else 0 end) as Household_Clusters_28,
	(case when Household_Clusters like '29%' then 1 else 0 end) as Household_Clusters_29,
	(case when Household_Clusters like '30%' then 1 else 0 end) as Household_Clusters_30,
	(case when Household_Clusters like '31%' then 1 else 0 end) as Household_Clusters_31,
	(case when Household_Clusters like '32%' then 1 else 0 end) as Household_Clusters_32,
	(case when Household_Clusters like '33%' then 1 else 0 end) as Household_Clusters_33,
	(case when Household_Clusters like '34%' then 1 else 0 end) as Household_Clusters_34,
	(case when Household_Clusters like '35%' then 1 else 0 end) as Household_Clusters_35,
	(case when Household_Clusters like '36%' then 1 else 0 end) as Household_Clusters_36,
	(case when Household_Clusters like '37%' then 1 else 0 end) as Household_Clusters_37,
	(case when Household_Clusters like '38%' then 1 else 0 end) as Household_Clusters_38,
	(case when Household_Clusters like '39%' then 1 else 0 end) as Household_Clusters_39,
	(case when Household_Clusters like '40%' then 1 else 0 end) as Household_Clusters_40,
	(case when Household_Clusters like '41%' then 1 else 0 end) as Household_Clusters_41,
	(case when Household_Clusters like '42%' then 1 else 0 end) as Household_Clusters_42,
	(case when Household_Clusters like '43%' then 1 else 0 end) as Household_Clusters_43,
	(case when Household_Clusters like '44%' then 1 else 0 end) as Household_Clusters_44,
	(case when Household_Clusters like '45%' then 1 else 0 end) as Household_Clusters_45,
	(case when Household_Clusters like '46%' then 1 else 0 end) as Household_Clusters_46,
	(case when Household_Clusters like '47%' then 1 else 0 end) as Household_Clusters_47,
	(case when Household_Clusters like '48%' then 1 else 0 end) as Household_Clusters_48,
	(case when Household_Clusters like '49%' then 1 else 0 end) as Household_Clusters_49,
	(case when Household_Clusters like '50%' then 1 else 0 end) as Household_Clusters_50,
	(case when Household_Clusters like '51%' then 1 else 0 end) as Household_Clusters_51,
	(case when Household_Clusters like '52%' then 1 else 0 end) as Household_Clusters_52,
	(case when Household_Clusters like '53%' then 1 else 0 end) as Household_Clusters_53,
	(case when Household_Clusters like '54%' then 1 else 0 end) as Household_Clusters_54,
	(case when Household_Clusters like '55%' then 1 else 0 end) as Household_Clusters_55,
	(case when Household_Clusters like '56%' then 1 else 0 end) as Household_Clusters_56,
	(case when Household_Clusters like '57%' then 1 else 0 end) as Household_Clusters_57,
	(case when Household_Clusters like '58%' then 1 else 0 end) as Household_Clusters_58,
	(case when Household_Clusters like '59%' then 1 else 0 end) as Household_Clusters_59,
	(case when Household_Clusters like '60%' then 1 else 0 end) as Household_Clusters_60,
	(case when Household_Clusters like '61%' then 1 else 0 end) as Household_Clusters_61,
	(case when Household_Clusters like '62%' then 1 else 0 end) as Household_Clusters_62,
	(case when Household_Clusters like '63%' then 1 else 0 end) as Household_Clusters_63,
	(case when Household_Clusters like '64%' then 1 else 0 end) as Household_Clusters_64,
	(case when Household_Clusters like '65%' then 1 else 0 end) as Household_Clusters_65,
	(case when Household_Clusters like '66%' then 1 else 0 end) as Household_Clusters_66,
	(case when Household_Clusters like '67%' then 1 else 0 end) as Household_Clusters_67,
	(case when Household_Clusters like '68%' then 1 else 0 end) as Household_Clusters_68,
	(case when Household_Clusters like '69%' then 1 else 0 end) as Household_Clusters_69,
	(case when Household_Clusters like '70%' then 1 else 0 end) as Household_Clusters_70,
	--(case when Insurance_Groups<>'' then CAST(SUBSTRING(Insurance_Groups,0,3) as int) else 0 end) as Insurance_Groups,
	(case when Insurance_Groups like '01%' then 1 else 0 end) as Insurance_Secured_Prosperity,
	(case when Insurance_Groups like '02%' then 1 else 0 end) as Insurance_Stable_Singles,
	(case when Insurance_Groups like '03%' then 1 else 0 end) as Insurance_Single_Opportunities,
	(case when Insurance_Groups like '04%' then 1 else 0 end) as Insurance_Pennywise_Homeowners,
	(case when Insurance_Groups like '05%' then 1 else 0 end) as Insurance_Parenting_Priorities,
	(case when Insurance_Groups like '06%' then 1 else 0 end) as Insurance_Comfortable_Renters,
	(case when Insurance_Groups like '07%' then 1 else 0 end) as Insurance_Retirement_Requirements,
	(case when Insurance_Groups like '08%' then 1 else 0 end) as Insurance_Prosperous_Families,
	(case when Insurance_Groups like '09%' then 1 else 0 end) as Insurance_Comfortable_Empty,
	(case when Insurance_Groups like '10%' then 1 else 0 end) as Insurance_Modest_Maturity,
	(case when Insurance_Groups like '11%' then 1 else 0 end) as Insurance_Modest_Country,
	(case when Insurance_Groups like '12%' then 1 else 0 end) as Insurance_Aspiring_Affluence,
	(case when Insurance_Groups like '13%' then 1 else 0 end) as Insurance_Pennywise_Renters,
	--(case when Financial_Groups<>'' then CAST(SUBSTRING(Financial_Groups,0,3) as int) else 0 end) as Financial_Groups,
	(case when Financial_Groups like '01%' then 1 else 0 end) as Financial_Urban_Investors,
	(case when Financial_Groups like '02%' then 1 else 0 end) as Financial_Suburban_Investors,
	(case when Financial_Groups like '03%' then 1 else 0 end) as Financial_Cautious_Planners,
	(case when Financial_Groups like '04%' then 1 else 0 end) as Financial_Safety_First,
	(case when Financial_Groups like '05%' then 1 else 0 end) as Financial_Savvy_Investors,
	(case when Financial_Groups like '06%' then 1 else 0 end) as Financial_Country_Caution,
	(case when Financial_Groups like '07%' then 1 else 0 end) as Financial_New_Market_Singles,
	(case when Financial_Groups like '08%' then 1 else 0 end) as Financial_Cash_and_Carry_Urbanites,
	(case when Financial_Groups like '09%' then 1 else 0 end) as Financial_Cash_and_Carry_Suburbanites,
	(case when Financial_Groups like '10%' then 1 else 0 end) as Financial_Rural_Security,
	(case when Financial_Groups like '11%' then 1 else 0 end) as Financial_Getting_Started,
	(case when Financial_Groups like '12%' then 1 else 0 end) as Financial_Financially_Challenged,
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