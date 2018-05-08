USE [OCM2]
GO

/****** Object:  StoredProcedure [dbo].[OCM4aReptanan]    Script Date: 5/7/2018 7:55:12 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		<Madhumitha Ravi>
-- Create date: <26-03-2018>
-- Version: 0.5
-- =============================================
CREATE PROCEDURE [dbo].[OCM4aReptanan]
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
	declare @Startdate date
	declare @Enddate date
	select @Startdate = StartDate,@EndDate = EndDate from MeasurementPeriod where IsActive = 1
	;with final as (select distinct a.*,
	-- This is for taking closest chemo date to dxdate before within 30days its date between encounterdate and -30days from Encounterdate--

				(select 
				     -- this Code is to find the EncounterCode for a LatestEncounterDate which is taken from Below Code
						(select distinct Top 1 convert(nvarchar,x.EncounterDate,110)+','+e1.EncounterCode+'.' 
							from Encounter e1
							where e1.EncounterCode in (SELECT ocs.Code FROM  OCMTechSpecValues ocs
							JOIN OCMTechSpec ots ON ocs.OCMTechSpecId = ots.Id 
							JOIN DataElements de ON ots.DataElementId = de.Id 
							JOIN Measure m ON ots.MeasureId = m.Id
							where m.Code like 'OCM-4a' and de.DataElementName like 'Chemotherapy' 
							and ocs.Code in ('96413','96409','96416','96401','96402'))
							and e1.EncounterDate = x.EncounterDate and e1.MRN =a.MRN)
						from
						-- this Code is to find the Latest EncounterDate for an MRN
						(select distinct  Max(e1.Encounterdate)EncounterDate 
							from Encounter e1
							where e1.EncounterCode in (SELECT ocs.Code FROM  OCMTechSpecValues ocs
							JOIN OCMTechSpec ots ON ocs.OCMTechSpecId = ots.Id 
							JOIN DataElements de ON ots.DataElementId = de.Id 
							JOIN Measure m ON ots.MeasureId = m.Id
							where m.Code like 'OCM-4a' and de.DataElementName like 'Chemotherapy' 
							and ocs.Code in ('96413','96409','96416','96401','96402'))
							and (e1.EncounterDate between a.previous and a.EncounterDate) and e1.MRN =a.MRN )x )FirstChemoDAte,

	-- This is for taking closest chemo date to dxdate after within 30days its date between encounterdate and +30days from Encounterdate--

				(select 
				 -- this Code is to find the EncounterCode for a LatestEncounterDate which is taken from Below Code
					(select distinct Top 1 convert(nvarchar,y.Encounterdate,110)+','+e1.EncounterCode+'.' 
						from Encounter e1
						where e1.EncounterCode in (SELECT ocs.Code FROM OCMTechSpecValues ocs 
						JOIN OCMTechSpec ots ON ocs.OCMTechSpecId = ots.Id 
						JOIN DataElements de ON ots.DataElementId = de.Id 
						JOIN Measure m ON ots.MeasureId = m.Id
						where m.Code like 'OCM-4a' and de.DataElementName like 'Chemotherapy' 
						and ocs.Code in ('96413','96409','96416','96401','96402'))
						and e1.EncounterDate =y.EncounterDate and e1.MRN = a.MRN)
					 from
					 -- this Code is to find the Latest EncounterDate for an MRN
					 (select distinct MIn(e1.Encounterdate)EncounterDate 
						from Encounter e1
						where e1.EncounterCode in (SELECT ocs.Code FROM OCMTechSpecValues ocs 
						JOIN OCMTechSpec ots ON ocs.OCMTechSpecId = ots.Id 
						JOIN DataElements de ON ots.DataElementId = de.Id 
						JOIN Measure m ON ots.MeasureId = m.Id
						where m.Code like 'OCM-4a' and de.DataElementName like 'Chemotherapy' 
						and ocs.Code in ('96413','96409','96416','96401','96402'))
						and (e1.EncounterDate between a.EncounterDate and a.after) and e1.MRN =a.MRN)y)SecondChemoDate,

	-- This is for taking closest chemo date to dxdate before within 30days its date between encounterdate before date and -30days from Encounterdate--

				(select 
				-- this Code is to find the EncounterCode for a LatestEncounterDate which is taken from Below Code
					(select distinct Top 1 convert(nvarchar,z.Encounterdate,110)+','+e1.EncounterCode+'.' 
						from Encounter e1
						where e1.EncounterCode in (SELECT ocs.Code FROM  OCMTechSpecValues ocs
						JOIN OCMTechSpec ots ON ocs.OCMTechSpecId = ots.Id 
						JOIN DataElements de ON ots.DataElementId = de.Id 
						JOIN Measure m ON ots.MeasureId = m.Id
						where m.Code like 'OCM-4a' and de.DataElementName like 'Chemotherapy' 
						and ocs.Code in ('96413','96409','96416','96401','96402'))
						and e1.EncounterDate = z.Encounterdate and e1.MRN = a.MRN) 
					from 
					-- this Code is to find the Latest EncounterDate for an MRN
					(select distinct  Max(e1.Encounterdate)Encounterdate 
						from Encounter e1
						where e1.EncounterCode in (SELECT ocs.Code FROM  OCMTechSpecValues ocs
						JOIN OCMTechSpec ots ON ocs.OCMTechSpecId = ots.Id 
						JOIN DataElements de ON ots.DataElementId = de.Id 
						JOIN Measure m ON ots.MeasureId = m.Id
						where m.Code like 'OCM-4a' and de.DataElementName like 'Chemotherapy' 
						and ocs.Code in ('96413','96409','96416','96401','96402'))
						and (e1.EncounterDate between a.previous and DATEADD(Day,-1,a.EncounterDate)) and e1.MRN =a.MRN)z)FirstChemoDAtebefEncDate,

	-- This is for taking closest chemo date to dxdate after within 30days its date between encounterdate after date and +30days from Encounterdate--

				(select 
				-- this Code is to find the EncounterCode for a LatestEncounterDate which is taken from Below Code
					(select distinct Top 1 Convert(nvarchar,a1.Encounterdate,110)+','+e1.EncounterCode+'.' 
						from Encounter e1
						where e1.EncounterCode in (SELECT ocs.Code FROM  OCMTechSpecValues ocs
						JOIN OCMTechSpec ots ON ocs.OCMTechSpecId = ots.Id 
						JOIN DataElements de ON ots.DataElementId = de.Id 
						JOIN Measure m ON ots.MeasureId = m.Id
						where m.Code like 'OCM-4a' and de.DataElementName like 'Chemotherapy' 
						and ocs.Code in ('96413','96409','96416','96401','96402'))
						and e1.EncounterDate = a1.EncounterDate and e1.MRN =a.MRN)
					from
					-- this Code is to find the Latest EncounterDate for an MRN
					(select distinct MIn(e1.Encounterdate)EncounterDate 
						from Encounter e1
						where e1.EncounterCode in (SELECT ocs.Code FROM  OCMTechSpecValues ocs
						JOIN OCMTechSpec ots ON ocs.OCMTechSpecId = ots.Id 
						JOIN DataElements de ON ots.DataElementId = de.Id 
						JOIN Measure m ON ots.MeasureId = m.Id
						where m.Code like 'OCM-4a' and de.DataElementName like 'Chemotherapy' 
						and ocs.Code in ('96413','96409','96416','96401','96402'))
						and (e1.EncounterDate between DATEADD(Day,+1,a.EncounterDate) and a.after) and e1.MRN =a.MRN)a1)SecondChemoDateaftEncDate,
				case when b.EncounterDate is not null and b.EncounterCode <> '1125F 8P' then 'Yes' else 'No' end PainintensityPresentnot,case when b.EncounterCode ='1125F' then 'Yes' else case when (b.EncounterCode ='1125F 8P' or b.EncounterCode is null) then '' else 'No' end end Painpresent,case when (b.EncounterCode is null or b.EncounterCode = '1125F 8P') then'' else  isnull(convert(nvarchar,b.EncounterDate,110),'') end  PainIntensityDate,case when (b.EncounterCode ='1125F 8P' or b.EncounterCode is null) then '' else isnull(b.EncounterCode,'') end painCOde 
		from 
			-- This Part is for taking the encounter visit Date for Measurement Period--
			(select distinct e.MRN,d.HICN,case when p.Status ='Active' then 'Yes' else 'No' end Activediagnosis,p.DxDate,p.ICD9Code,
				 e.EncounterDate,e.EncounterCode,e.EncounterCodeSystem,DATEADD(DAY,-30,e.EncounterDate)previous,DATEADD(DAY,+30,e.EncounterDate)after from Encounter e
				join PatientDx p on p.MRN = e.MRN
				join Demographics d on d.MRN=e.MRN
				where  e.EncounterCode in (SELECT OCMTechSpecValues.Code FROM  OCMTechSpecValues 
								JOIN OCMTechSpec ON OCMTechSpecValues.OCMTechSpecId = OCMTechSpec.Id 
								JOIN DataElements ON OCMTechSpec.DataElementId = DataElements.Id 
								JOIN Measure ON OCMTechSpec.MeasureId = Measure.Id
								join CodeSystem cs on OCMTechSpecValues.CodeSystemId = cs.Id
								where Measure.Code like 'OCM-4a' and DataElements.DataElementName like 'Encounter' and cs.CodeName = 'CPT' )
								and p.ICD9Code in (select Code from CancerCodeList)
								and p.Ranking='primary' and d.PatStatus = 'Active' 
								and (e.EncounterDate between @Startdate and @Enddate) )a
			-- This part is for taking Pain details available within the  Measurement Period-- 
			 left outer join (select distinct e.MRN,e.EncounterId,e.EncounterDate,e.EncounterCodeSystem,e.EncounterCode from Encounter e
							 join PatientDx p on p.MRN = e.MRN
							 join Demographics d on d.MRN=e.MRN
							 where p.Ranking='primary' and d.PatStatus = 'Active'  and e.EncounterCode in ('1125f','1126f','1125F 8P')
							 and p.ICD9Code in (select Code from CancerCodeList)
							 and (e.EncounterDate between @Startdate and @Enddate)) b on a.MRN =b.MRN and a.EncounterDate = b.EncounterDate
			 )
	select distinct f.MRN cola,
	f.HICN colb,
	f.Activediagnosis colc,
	isnull(f.DxDate,'') cold,
	'' cole,
	'' colf,
	f.ICD9Code colg,
	'' colh,
	'' coli,
	'' colj,
	'' colk,
	case when ((f.FirstChemoDAtebefEncDate is not null and f.SecondChemoDateaftEncDate is not null)or(f.FirstChemoDAte is not null and f.SecondChemoDate is not null)) then 'Yes' else 'No' end coll,
	f.EncounterDate colm,
	'' coln,
	f.EncounterCode colo,
	case when f.FirstChemoDAtebefEncDate is not null then isnull(Substring(f.FirstChemoDAtebefEncDate,1,CHARINDEX(',',f.FirstChemoDAtebefEncDate)-1),'') else isnull(Substring(f.FirstChemoDAte,1,CHARINDEX(',',f.FirstChemoDAte)-1),'') end colp,
	''colq,
	case when f.FirstChemoDAtebefEncDate is not null then isnull(Replace(Substring(f.FirstChemoDAtebefEncDate,CHARINDEX(',',f.FirstChemoDAtebefEncDate)+1,(CHARINDEX('.',f.FirstChemoDAtebefEncDate)-1)),'.',''),'') else  isnull(Replace(Substring(f.FirstChemoDAte,CHARINDEX(',',f.FirstChemoDAte)+1,(CHARINDEX('.',f.FirstChemoDAte)-1)),'.',''),'') end colr,
	case when f.SecondChemoDateaftEncDate is not null then isnull(Substring(f.SecondChemoDateaftEncDate,1,CHARINDEX(',',f.SecondChemoDateaftEncDate)-1),'') else isnull(Substring(f.SecondChemoDate,1,CHARINDEX(',',f.SecondChemoDate)-1),'') end cols,
	'' colt,
	case when f.SecondChemoDateaftEncDate is not null then isnull(Replace(Substring(f.SecondChemoDateaftEncDate,CHARINDEX(',',f.SecondChemoDateaftEncDate)+1,(CHARINDEX('.',f.SecondChemoDateaftEncDate)-1)),'.',''),'') else isnull(Replace(Substring(f.SecondChemoDate,CHARINDEX(',',f.SecondChemoDate)+1,(CHARINDEX('.',f.SecondChemoDate)-1)),'.',''),'') end colu,
	f.PainintensityPresentnot colv,
	f.Painpresent colw,
	f.PainIntensityDate colx,
	''coly,
	f.painCOde colz
	--case when (f.DxDate is not null and f.EncounterDate is not null and ((f.FirstChemoDAte is not null and f.FirstChemoDAte <> '') or (f.FirstChemoDAtebefEncDate is not null and f.FirstChemoDAtebefEncDate <> '')) and ((f.SecondChemoDate is not null and f.SecondChemoDate <>'')or(f.SecondChemoDateaftEncDate is not null and f.SecondChemoDateaftEncDate <>''))) then 1 else 0 end Den,
	--case when (f.DxDate is not null and f.EncounterDate is not null and ((f.FirstChemoDAte is not null and f.FirstChemoDAte <> '') or (f.FirstChemoDAtebefEncDate is not null and f.FirstChemoDAtebefEncDate <> '')) and ((f.SecondChemoDate is not null and f.SecondChemoDate <>'')or(f.SecondChemoDateaftEncDate is not null and f.SecondChemoDateaftEncDate <>'')) and f.PainIntensityDate is not null and f.PainIntensityDate <> '') then 1 else 0 end Num 
	from final f 
	where (f.FirstChemoDAtebefEncDate is not null or f.FirstChemoDAte is not null )
and (f.SecondChemoDateaftEncDate is not null or f.SecondChemoDate is not null) and f.ICD9Code in (select code from CancerCodeList) and f.MRN in (select distinct MRN from Meos where CalanderMonth between @Startdate and @Enddate and MEOSEligible is null)
END
GO


