with itg_udcdetails as(
	select * from {{ ref('inditg_integration__itg_udcdetails') }}
),
itg_udcmaster as(
	select * from {{ ref('inditg_integration__itg_udcmaster') }}
),
transformed as(
SELECT derived_table1.distcode AS customer_code_udc
	,derived_table1.mastervaluecode AS retailer_code_udc
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('HSACounter'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_hsacounter
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Key Account Name'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_keyaccountname
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('PharmacyChain'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_pharmacychain
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('OneDetailingBaby'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_onedetailingbaby
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('SanproVisibilitySSS'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_sanprovisibilitysss
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('SSSConsOffer'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_sssconsoffer
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('PlatinumClub2018'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_platinumclub2018
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('SSSEndCaps'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_sssendcaps
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('PlatinumClub2019'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_platinumclub2019
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Signature2019'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_signature2019
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Premium2019'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_premium2019
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('GSTN'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_gstn
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Platinum Q2 2019New'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_platinumq22019new
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('SSSProgram 2019'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_sssprogram2019
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Umang Q3 2019'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_umangq32019
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('SSSScheme2019'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_sssscheme2019
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('SSS Promoter 2019'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_ssspromoter2019
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('RtrTypeAttr'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_rtrtypeattr
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Bhagidari Q3 2019'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_bhagidariq32019
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Director Club Q3 2019'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_directorclubq32019
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Platinum Q3 2019New'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_platinumq32019new
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Platinum Q2 2019'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_platinumq22019
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Platinum Q3 2019'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_platinumq32019
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('BBAStore'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_bbastore
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('AvBabyBodyDocQ42019'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_avbabybodydocq42019
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('ORSL CAC 2019'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_orslcac2019
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Baby Profesional CAC 2019'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_babyprofesionalcac2019
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Platinum Q4 2019'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_platinumq42019
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('SchemeSSS2020'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_schemesss2020
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Platinum Q1 2020'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_platinumq12020
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Platinum Q3 2020'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_platinumq32020
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('SSS Q3 2020'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_sssq32020
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Bhagidari Q3 2020'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_bhagidariq32020
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Director Club Q3 2020'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_directorclubq32020
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Umang Q3 2020'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_umangq32020
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Daud Q3 2020'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_daudq32020
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Daud Q4 2020'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_daudq42020
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Platinum Q4 2020'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_platinumq42020
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Director Club Q4 2020'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_directorclubq42020
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('SSS Program Q4 2020'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_sssprogramq42020
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Bhagidari Q4 2020'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_bhagidariq42020
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Umang Q4 2020'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_umangq42020
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Samriddhi'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_samriddhi
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('SSS Program Q1 2021'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_sssprogramq12021
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Platinum Q1 2021'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_platinumq12021
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('ORSL CAC 2021'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_orslcac2021
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Bhagidari Q1 2021'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_bhagidariq12021
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Daud Q1 2021'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_daudq12021
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Director Club Q1 2021'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_directorclubq12021
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Umang Q1 2021'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_umangq12021
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Platinum Q2 2021'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_platinumq22021
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('SSS Program Q2 2021'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_sssprogramq22021
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Bhagidari Q2 2021'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_bhagidariq22021
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Daud Q2 2021'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_daudq22021
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Director Club Q2 2021'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_directorclubq22021
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Umang Q2 2021'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_umangq22021
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Platinum Q3 2021'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_platinumq32021
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('SSS Program Q3  2021'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_sssprogramq32021
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('New GTM'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_newgtm
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('SSS Program Q1 2022'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_sssprogramq12022
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('SSS Pharmacy Store'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_ssspharmacystore
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('SSS ToT Stores'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_ssstotstores
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Platinum Q1 2022'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_platinumq12022
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('HSA Counter Q1 2022'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_hsacounterq12022
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('BSSAveenoUDC2022'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_bssaveenoudc2022
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('SSS Program Q2 2022'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_sssprogramq22022
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Platinum Q2 2022'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_platinumq22022
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('HSA Counter Q2 2022'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_hsacounterq22022
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Ecommerce'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_ecommerce
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Baby Top Store Activation'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_babytopstoreactivation
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Platinum Q3 2022'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_platinumq32022
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('HSA Counter Q3 2022'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_hsacounterq32022
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Platinum Q4 2022'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_platinumq42022
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('SSS Program Q4 2022'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_sssprogramq42022
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('SSS Program Q1 2023'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_sssprogramq12023
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('HSA Counter Q1 2023'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_hsacounterq12023
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Platinum Q1 2023'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_platinumq12023
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Aarogyam'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_aarogyam
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('SSS Program Q2 2023'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_sssprogramq22023
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('SSS- Hyper Stores 2023'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_ssshyperstores2023
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Win@Birth2023'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_winbirth2023
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Win@Clinic2023'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_winclinic2023
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('HSA Counter Q2 2023'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_hsacounterq22023
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('HSA Counter Q3 2023'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_hsacounterq32023
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Aveeno SSS Stores'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_aveenosssstores
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('HSA Counter Q4 2023'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_hsacounterq42023
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('HSA Counter Q1 2024'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_hsacounterq12024
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('Q1 24 BSS program'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_q124bssprogram
	,"max" (
		(
			CASE 
				WHEN ((derived_table1.columnname)::TEXT = ('SpecialtyProfessional2024'::CHARACTER VARYING)::TEXT)
					THEN (derived_table1.columnvalue)::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END
			)::TEXT
		) AS udc_specialtyprofessional2024
	,convert_timezone('UTC', current_timestamp()) AS crt_dttm
	,convert_timezone('UTC', current_timestamp()) AS updt_dttm
FROM (
	SELECT udc_1.distcode
		,udc_1.mastervaluecode
		,udc_1.mastervaluename
		,udc_1.columnname
		,udc_1.columnvalue
		,udc_1.rn
	FROM (
		SELECT itg_udcdetails.distcode
			,itg_udcdetails.mastervaluecode
			,upper((itg_udcdetails.mastervaluename)::TEXT) AS mastervaluename
			,itg_udcdetails.columnname
			,CASE 
				WHEN (
						(itg_udcdetails.columnvalue IS NULL)
						OR (trim((itg_udcdetails.columnvalue)::TEXT) = (''::CHARACTER VARYING)::TEXT)
						)
					THEN (NULL::CHARACTER VARYING)::TEXT
				ELSE upper((itg_udcdetails.columnvalue)::TEXT)
				END AS columnvalue
			,row_number() OVER (
				PARTITION BY itg_udcdetails.distcode
				,itg_udcdetails.mastervaluecode
				,itg_udcdetails.columnname ORDER BY itg_udcdetails.createddate DESC
					,itg_udcdetails.columnvalue DESC NULLS LAST
				) AS rn
		FROM (
			itg_udcdetails itg_udcdetails LEFT JOIN itg_udcmaster udcmaster ON (((itg_udcdetails.columnname)::TEXT = (udcmaster.columnname)::TEXT))
			)
		WHERE (
				((itg_udcdetails.mastername)::TEXT = ('Retailer Master'::CHARACTER VARYING)::TEXT)
				AND (udcmaster.udcstatus = 1)
				)
		) udc_1
	WHERE (udc_1.rn = 1)
	) derived_table1
GROUP BY derived_table1.distcode
	,derived_table1.mastervaluecode
)
select * from transformed