package ke.tu.dl.localmodel

import ke.tu.dl.utils.ProductionDBConn._
import scalikejdbc._

/**
 * Contact query model
 */
case class QContact(
  crn: Long,
  isCompany: Boolean,
  surname: String,
  otherNames: String,
  nationalId: Option[String],
  passportId: Option[String],
  serviceId: Option[String],
  alienId: Option[String],
  dob: Option[java.sql.Date],
  isDeleted: Boolean)

case class QIdentification(id: Long, crn: Long)

/**
 * Account summary query model
 */
case class QAccountSummary(
  accountId: Long,
  parentCrn: Long,
  dateUpdated: java.sql.Date,
  accountNumber: String,
  subscriberId: Integer,
  isAccountDeleted: Option[Boolean],
  statusCode: Int,
  typeCode: Int,
  currentBalance: Double,
  principalAmount: Double,
  lastPaymentDate: java.sql.Date)

/**
 * Contact persistence model
 */
case class PContact(
  isCompany: Boolean,
  salutation: String,
  surname: String,
  otherNames: String,
  guid: String,
  nationalId: String,
  passportId: String,
  serviceId: String,
  alienId: String,
  gender: String,
  dateOfBirth: java.sql.Date,
  dateOfDeath: java.sql.Date,
  maritalStatus: Integer,
  pin: String,
  nationality: Integer,
  vatId: String,
  taxId: String,
  companyRegNum: String,
  companyRegDate: java.sql.Date,
  companyCeaseDate: java.sql.Date,
  ceaseSource: String,
  numDependants: Integer,
  placeOfBirth: String,
  fileOpenDate: java.sql.Date,
  lastUpdateDate: java.sql.Date,
  dataSourceId: Long,
  numDirectors: String,
  numShareholders: String,
  status: String,
  statusDate: java.sql.Date,
  groupNum: String,
  companyType: String,
  annualTurnover: String,
  industryCode: String,
  isDeleted: Boolean)

/**
 *  Identification persistence model
 */
case class PIdentification(
  id: Option[Long],
  crn: Long,
  nationalId: String,
  taxId: String,
  vatId: String,
  passportNumber: String,
  nationality: Int,
  alienId: String,
  serviceId: String,
  companyRegNum: String,
  dataSourceId: Long,
  status: String,
  statusDate: java.sql.Date,
  dob: java.sql.Date,
  companyRegDate: java.sql.Date,
  lastUpdateDate: java.sql.Date,
  annualTurnover: String,
  companyType: String,
  industry: String)

/**
 * Account persistence model
 */
case class PAccount(
  parentCrn: Long,
  bankListing: Long,
  subscriberId: Int,
  userId: Long,
  sectorId: Long,
  accountNumber: String,
  accountTypeId: Int,
  accountStatusId: Int,
  accountOwnershipId: Int,
  currencyId: Int,
  dateOpened: java.sql.Date,
  dateUpdated: java.sql.Date,
  creditLimit: Double,
  principalAmount: Double,
  termsDuration: String,
  paymentTermId: Int,
  scheduledMonthlyAmount: Option[Double],
  actualMonthlyAmount: Option[Double],
  collateralType: Option[String],
  currentBalance: Double,
  pastDueAmount: Double,
  lastPaymentDate: java.sql.Date,
  dataSourceId: Int,
  guid: String,
  sectorOfActivity: String,
  finalPaymentDate: java.sql.Date,
  accountHolderTypeId: Int,
  statusDate: java.sql.Date,
  closureReason: String,
  pastDueDate: java.sql.Date,
  numberOfInstallmentsInArrears: Int,
  numberOfDaysInArrears: Int,
  interestRate: Option[Float],
  firstPaymentDate: java.sql.Date,
  firstPaymentAmount: Option[Double],
  securityType: String,
  lastPaymentAmount: Double,
  listingDate: java.sql.Date,
  exchangeRate: Option[Float],
  summaryId: Long,
  annualTurnoverBand: String,
  deferredPaymentAmount: Double,
  deferredPaymentDate: java.sql.Date,
  disbursedAmount: Option[Double],
  disbursementDate: java.sql.Date,
  dueDate: java.sql.Date,
  jointAccountIndicator: String,
  localCurrencyAmount: Double,
  accountIndicator: String)

/**
 * Account summary persistence model
 */
case class PAccountSummary(
  accountId: Option[Long],
  parentCrn: Long,
  accountNumber: String,
  statusCode: Integer,
  typeCode: Integer,
  ownerCode: Integer,
  subscriberId: Integer,
  currentBalance: Double,
  listingDate: java.sql.Date,
  principalAmount: Double,
  lastPaymentDate: java.sql.Date)

/**
 * Contact Query model API
 */
object ContactQuery {

  def searchContact(isCompany: Boolean, idParam: String, paramType: String,
    nameParam: String): List[QContact] = {
    import scala.collection.mutable.ListBuffer
    var searchResult: ListBuffer[QContact] = new ListBuffer[QContact]()

    if (isCompany == true) {
      val cis = getCIContact(idParam)
      for (ci <- cis) {
        if ((ci.surname != null) && (ci.surname.replaceAll("[^A-Za-z0-9]", "").trim.toUpperCase equals
          nameParam.replaceAll("[^A-Za-z0-9]", "").trim.toUpperCase))
          searchResult.append(ci)
      }

    } else if (isCompany == false) {
      val ics = getICContact(idParam)
      for (ic <- ics) {
        if ((ic.surname != null && nameParam != null) && (ic.surname.replaceAll("[^A-Za-z0-9]", "").trim.toUpperCase contains nameParam
          .replaceAll("[^A-Za-z0-9]", "").trim.toUpperCase) ||
          (nameParam.replaceAll("[^A-Za-z0-9]", "").trim.toUpperCase contains ic.surname
            .replaceAll("[^A-Za-z0-9]", "").trim.toUpperCase))
          if ((ic.otherNames != null && nameParam != null) && (ic.otherNames.replaceAll("[^A-Za-z0-9]", "").trim.toUpperCase contains nameParam
            .replaceAll("[^A-Za-z0-9]", "").trim.toUpperCase) ||
            (nameParam.replaceAll("[^A-Za-z0-9]", "").trim.toUpperCase contains ic.otherNames
              .replaceAll("[^A-Za-z0-9]", "").trim.toUpperCase))
            searchResult.append(ic)
      }
    }

    searchResult.toList
  }

  /**
   * For a specified company registration number, retrieves a contact
   */
  def getCIContact(companyRegNum: String): List[QContact] = {
    using(DB(getDBConn)) { db =>
      db readOnly { implicit session =>
        sql"""
	      	select 
	        	i.Ident_CRN as crn,
	        	c.Contact_Surname as surname,
	        	c.Contact_OtherNames as otherNames,
	        	c.Contact_IsCompany as isCompany,
	        	c.Contact_Deleted as isDeleted  
	        FROM Contacts_Identification as i  
	        INNER JOIN Contacts as c 
	        ON i.Ident_CRN = c.Contact_CRN 
	        WHERE replace(replace(i.Ident_CoRegNo, 'C', ''), '.', '') = ${companyRegNum}   
	        AND c.Contact_Deleted = ${false}
      	""".map(rs => QContact(
          rs.long("crn"),
          rs.boolean("isCompany"),
          rs.string("surname"),
          rs.string("otherNames"),
          None,
          None,
          None,
          None,
          None,
          rs.boolean("isDeleted"))).list.apply
      }
    }
  }

  /**
   * For either nationalId, passport Number, serviceId or alienId, retrieves contact
   */
  def getICContact(nationalId: String): List[QContact] = {
    using(DB(getDBConn)) { db =>
      db readOnly { implicit session =>
        sql"""
	      	select distinct  
	        	i.Ident_CRN as crn,
	        	c.Contact_Surname as surname,
	        	c.Contact_OtherNames as otherNames,
	        	c.Contact_IsCompany as isCompany,
	        	c.Contact_Deleted as isDeleted,
	        	c.Contact_NationalID as nationalId,
	        	c.Contact_PassportNo as passportId,
	        	c.Contact_ServiceID as serviceId,
	        	c.Contact_AlienID as alienId,
	        	c.Contact_DOB as dob 
	        FROM Contacts_Identification as i 
	        INNER JOIN Contacts as c 
	        ON i.Ident_CRN = c.Contact_CRN 
	        WHERE i.Ident_NationalID = ${nationalId} 
	        AND c.Contact_Deleted = ${false} 
      	""".map(rs => QContact(
          rs.long("crn"),
          rs.boolean("isCompany"),
          rs.string("surname"),
          rs.string("otherNames"),
          Some(rs.string("nationalId")),
          Some(rs.string("passportId")),
          Some(rs.string("serviceId")),
          Some(rs.string("alienId")),
          Some(rs.date("dob")),
          rs.boolean("isDeleted"))).list.apply
      }
    }
  }

  /**
   * Retrieves an Identification with the specified CRN
   */
  def getIdentification(crn: Long): Option[QIdentification] = {
    using(DB(getDBConn)) { db =>
      db readOnly { implicit session =>
        sql"""
	  			select distinct Ident_ID as id  
	  			from Contacts_Identifcation 
	  			where Ident_CRN = ${crn}
	  		""".map(rs => QIdentification(rs.long("id"), crn)).first.apply
      }
    }
  }

  /**
   * Retrieves an account summary with the specified account number and subscriberId
   */
  def getAccountSummary(parentCrn: Long, accountNumber: String, subscriberId: Integer): Option[QAccountSummary] = {
    using(DB(getDBConn)) { db =>
      db readOnly { implicit session =>
        try
          sql"""
        	select  
        		Account_ID as accountId,
        		Account_Deleted as isDeleted,
        		Account_DateUpdated as dateUpdated,
        		Account_Status as statusCode,
        		Account_Type as typeCode,
        		Account_CurrentBalanceAmt as currentBalance,
        		Account_PrincipalAmount as principalAmount,
        		Account_LastPaymentDate as lastPaymentDate 
        FROM Accounts 
        WHERE Account_SubscriberID = ${subscriberId} 
        AND Account_Reference = ${accountNumber} 
        AND Account_CRN = ${parentCrn}
        """.map(rs => QAccountSummary(
            rs.long("accountId"),
            parentCrn,
            rs.date("dateUpdated"),
            accountNumber,
            subscriberId,
            Some(rs.boolean("isDeleted")),
            rs.int("statusCode"),
            rs.int("typeCode"),
            rs.double("currentBalance"),
            rs.double("principalAmount"),
            rs.date("lastPaymentDate"))).first.apply
        catch { case e: Exception => None }
      }
    }
  }
}

/**
 * Contact persistence model
 */
object ContactPersist {

  /**
   * Creates a new Contact record
   */
  def createContact(c: PContact): Long = using(DB(getDBConn)) { db =>
    db localTx { implicit session =>
      sql"""
    	  	insert into Contacts (
        		Contact_IsCompany,
        		Contact_Salutation,
        		Contact_Surname,
        		Contact_OtherNames,
        		Contact_GUID,
        		Contact_NationalID,
        		Contact_PassportNo,
        		Contact_ServiceID,
        		Contact_AlienID,
        		Contact_Sex,
        		Contact_DOB,
        		Contact_DOD,
        		Contact_MaritalStatus,
        		Contact_Nationality,
        		Contact_VATID,
        		Contact_TaxID,
        		Contact_CoRegNo,
        		Contact_CoRegistrationDate,
        		Contact_CoCeaseDate,
        		Contact_CeaseSource,
        		Contact_NoDependants,
        		Contact_PlaceOfBirth,
        		Contact_FileOpenDate,
        		Contact_LastUpdateDate,
        		Contact_DataSourceID,
        		Contact_NoOfDirectors,
        		Contact_NoOfShareholders,
        		Contact_Status,
        		Contact_StatusDate,
        		Contact_GroupNo,
        		Contact_CompanyType,
        		Contact_AnnualTurnover,
        		Contact_Industry,
        		Contact_Deleted
        	) values (
        		${c.isCompany},
        		${c.salutation},
        		${c.surname},
        		${c.otherNames},
        		${c.guid},
        		${c.nationalId},
        		${c.passportId},
        		${c.serviceId},
        		${c.alienId},
        		${c.gender},
        		${c.dateOfBirth},
        		${c.dateOfDeath},
        		${c.maritalStatus},
        		${c.nationality},
        		${c.vatId},
        		${c.taxId},
        		${c.companyRegNum},
        		${c.companyRegDate},
        		${c.companyCeaseDate},
        		${c.ceaseSource},
        		${c.numDependants},
        		${c.placeOfBirth},
        		${c.fileOpenDate},
        		${c.lastUpdateDate},
        		${c.dataSourceId},
        		${c.numDirectors},
        		${c.numShareholders},
        		${c.status},
        		${c.statusDate},
        		${c.groupNum},
        		${c.companyType},
        		${c.annualTurnover},
        		${c.industryCode},
        		${c.isDeleted}
        	)
    	  """
        .updateAndReturnGeneratedKey.apply
    }
  }

  /**
   * Creates a new Identification record
   */
  def createIdentification(id: PIdentification) = {
    using(DB(getDBConn)) { db =>
      db localTx { implicit session =>
        sql"""
      			insert into Contacts_Identification (
      				Ident_CRN,
      				Ident_NationalID,
      				Ident_TaxID,
      				Ident_VATID,
      				Ident_PassportNo,
      				Ident_Nationality,
      				Ident_CoRegNo,
      				Ident_DataSourceID,
      				Ident_AlienID,
      				Ident_ServiceID,
      				Ident_Status,
      				Ident_StatusDate,
      				Ident_DOB,
      				Ident_CoRegistrationDate,
      				Ident_LastUpdateDate,
      				Ident_AnnualTurnover,
      				Ident_CompanyType,
      				Ident_Industry
      			) values (
        			${id.crn},
        			${id.nationalId},
        			${id.taxId},
        			${id.vatId},
        			${id.passportNumber},
        			${id.nationality},
        			${id.companyRegNum},
        			${id.dataSourceId},
        			${id.alienId},
        			${id.serviceId},
        			${id.status},
        			${id.statusDate},
        			${id.dob},
        			${id.companyRegDate},
        			${id.lastUpdateDate},
        			${id.annualTurnover},
        			${id.companyType},
        			${id.industry}
        		)
      		""".update.apply
      }
    }
  }

  /**
   * Creates new Account Summary record
   */
  def createAccountSummary(as: PAccountSummary): Long =
    using(DB(getDBConn)) { db =>
      db localTx { implicit session =>
        sql"""
        		insert into Accounts_Summary (
        			Account_CRN,
        			Account_Ref,
        			Status_Code,
        			Type_Code,
        			SubscriberID,
        			CurrentBalanceAmt,
        			ListingDate,
        			PrincipalAmount,
        			LastPaymentDate
        		) values (
        			${as.parentCrn},
          			${as.accountNumber},
          			${as.statusCode},
          			${as.typeCode},
          			${as.subscriberId},
          			${as.currentBalance},
          			${as.listingDate},
          			${as.principalAmount},
          			${as.lastPaymentDate}
        		)
        	"""
          .updateAndReturnGeneratedKey.apply
      }
    }

  /**
   * Updates account summary
   */
  def updateAccountSummary(as: QAccountSummary) = {
    using(DB(getDBConn)) { db =>
      db localTx { implicit session =>
        sql"""
      		update Accounts_Summary set  
        		Status_Code = ${as.statusCode},
        		Type_Code = ${as.typeCode},
        		CurrentBalanceAmt = ${as.currentBalance},
        		PrincipalAmount = ${as.principalAmount},
        		LastPaymentDate = ${as.lastPaymentDate} 
        	where Account_ID = ${as.accountId}
      	"""
          .update.apply
      }
    }
  }

  /**
   * Creates a new account record
   */
  def createAccount(a: PAccount) = {
    using(DB(getDBConn)) { db =>
      db localTx { implicit session =>
        sql"""
        	insert into Accounts (
      			Account_CRN,
        		Account_BankListing,
        		Account_SubscriberID,
        		Account_UserID,
        		Account_SectorID,
        		Account_Reference,
        		Account_Type,
        		Account_Status,
        		Account_Ownership,
        		Account_CurrencyID,
        		Account_DateOpened,
        		Account_DateUpdated,
        		Account_CreditLimit,
        		Account_PrincipalAmount,
        		Account_TermsDuration,
        		Account_PaymentTerms,
        		Account_ScheduledMonthlyAmt,
        		Account_ActualMonthlyAmt,
        		Account_CollateralType,
        		Account_CurrentBalanceAmt,
        		Account_PastDueAmt,
        		Account_LastPaymentDate,
        		Account_DataSourceID,
        		Account_GUID,
        		Account_SectorOfActivity,
        		Account_FinalPaymentDate,
        		Account_HolderType,
        		Account_StatusDate,
        		Account_ClosureReason,
        		Account_PastDueDate,
        		Account_NoOfInstallmentsInArrears,
        		Account_NoOfDaysInArrears,
        		Account_InterestRate,
        		Account_FirstPaymentDate,
        		Account_FirstPaymentAmount,
        		Account_SecurityType,
        		Account_LastPaymentAmount,
        		Account_ListingDate,
        		Account_ExchRate,
        		Account_SummaryID,
        		Account_AnnualTurnoverBand,
        		Account_DeferredPaymentAmount,
        		Account_DeferredPaymentDate,
        		Account_DisbursedAmount,
        		Account_DisbursementDate,
        		Account_DueDate,
        		Account_JointIndicator,
        		Account_LocalCurrencyAmount,
        		Account_NPLIndicator
        	) values (
        		${a.parentCrn},
        		${a.bankListing},
        		${a.subscriberId},
        		${a.userId},
        		${a.sectorId},
        		${a.accountNumber},
        		${a.accountTypeId},
        		${a.accountStatusId},
        		${a.accountOwnershipId},
        		${a.currencyId},
        		${a.dateOpened},
        		${a.dateUpdated},
        		${a.creditLimit},
        		${a.principalAmount},
        		${a.termsDuration},
        		${a.paymentTermId},
        		${a.scheduledMonthlyAmount},
        		${a.actualMonthlyAmount},
        		${a.collateralType},
        		${a.currentBalance},
        		${a.pastDueAmount},
        		${a.lastPaymentDate},
        		${a.dataSourceId},
        		${a.guid},
        		${a.sectorOfActivity},
        		${a.finalPaymentDate},
        		${a.accountHolderTypeId},
        		${a.statusDate},
        		${a.closureReason},
        		${a.pastDueDate},
        		${a.numberOfInstallmentsInArrears},
        		${a.numberOfDaysInArrears},
        		${a.interestRate},
        		${a.firstPaymentDate},
        		${a.firstPaymentAmount},
        		${a.securityType},
        		${a.lastPaymentAmount},
        		${a.listingDate},
        		${a.exchangeRate},
        		${a.summaryId},
        		${a.annualTurnoverBand},
        		${a.deferredPaymentAmount},
        		${a.deferredPaymentDate},
        		${a.disbursedAmount},
        		${a.disbursementDate},
        		${a.dueDate},
        		${a.jointAccountIndicator},
        		${a.localCurrencyAmount},
        		${a.accountIndicator}
        		)
      	"""
          .update.apply
      }
    }
  }
}

/**
 * Contact utility methods
 */
object ContactUtils {
  import ke.tu.dl.localmodel.DBLookup._
  import ke.tu.dl.utils.Cfg.loadProperties

  /**
   * Load properties
   */
  val props = loadProperties

  /**
   * Define date format to be used
   */
  val dateFormat = new java.text.SimpleDateFormat("yyyyMMdd")

  /**
   * Creates a persistence contact object
   */
  def toPContact(r: Array[String], isCompany: Boolean): PContact = {
    // Fields to transform
    var subscriberId: Int = 0
    var nationalId = ""
    var passportId = ""
    var serviceId = ""
    var alienId = ""
    var companyRegNum = ""
    var gender = ""
    var maritalStatus = ""
    var tradingStatus = ""
    var companyType = ""
    var annualTurnover = ""
    var industryCode = ""
    var nationality = 0;
    var dateOfBirth: java.sql.Date = null
    var dateOfDeath: java.sql.Date = null
    var companyRegDate: java.sql.Date = null
    var companyCeaseDate: java.sql.Date = null
    var fileOpenDate: java.sql.Date = null
    var lastUpdateDate: java.sql.Date = null
    var statusDate: java.sql.Date = null

    // Identification document Local Lookup
    if (isCompany == false) {
      if (r(11) equals "001") nationalId = r(12)
      else if (r(11) equals "002") passportId = r(12)
      else if (r(11) equals "003") serviceId = r(12)
      else if (r(11) equals "004") alienId = r(12)
    } else if (r(11) equals "005") companyRegNum = r(12)

    // Gender Local Lookup
    if (isCompany == false) gender = r(8).trim
    else if (isCompany == true) gender = "I"

    // Marital Status Local Lookup
    if (isCompany == false) maritalStatus = props.getProperty("ke.maritalStatus." + r(10).trim.toLowerCase)

    // Trading Status Local Lookup
    if (isCompany == true) tradingStatus = props.getProperty("ke.tradingStatus." + r(14).trim.toLowerCase)

    // Company Type Local Lookup
    if (isCompany == true) companyType = props.getProperty("ke.companyType." + r(8).trim.toLowerCase)

    // Annual Turnover Local Lookup
    if (isCompany == true) annualTurnover = props.getProperty("ke.annualTurnoverBand." + r(10).trim.toLowerCase)

    // IndustryCode Local Lookup
    if (isCompany == false) industryCode = props.getProperty("ke.employerIndustryType." + r(34).trim.toLowerCase)
    else if (isCompany == true) industryCode = props.getProperty("ke.employerIndustryType." + r(9).trim.toLowerCase)

    // Nationality DB Lookup

    // Formatting
    try if (isCompany == false) dateOfBirth = new java.sql.Date(dateFormat.parse(r(5)).getTime) catch { case e: Exception => }
    try if (isCompany == true) companyRegDate = new java.sql.Date(dateFormat.parse(r(2)).getTime) catch { case e: Exception => }
    try if (isCompany == true) statusDate = new java.sql.Date(dateFormat.parse(r(15)).getTime) catch { case e: Exception => }
    fileOpenDate = new java.sql.Date((new java.util.Date).getTime)
    lastUpdateDate = new java.sql.Date((new java.util.Date).getTime)

    if (isCompany == false)
      new PContact(
        isCompany,
        r(4), // Salutation
        r(0), // Surname
        (r(1) + " " + r(2) + " " + r(3)).trim, // Other Names
        java.util.UUID.randomUUID().toString(),
        nationalId,
        passportId,
        serviceId,
        alienId,
        gender,
        dateOfBirth: java.sql.Date, // Date of Birth
        dateOfDeath: java.sql.Date,
        0, // Marital Status
        r(31), // PIN
        0, // Nationality
        "", // VAT ID
        "", // TAX
        companyRegNum,
        companyRegDate,
        companyCeaseDate,
        "", // Cease Source
        null, // Number of Dependants
        "", // Place of Birth
        fileOpenDate,
        lastUpdateDate,
        0, // Data Source ID
        "", // Number of Directors
        "", // Number of Shareholders
        "", // Status
        statusDate,
        "", // Group Number
        companyType,
        annualTurnover,
        industryCode,
        false) // ISDeleted
    else
      new PContact(
        isCompany,
        "", // Salutation
        r(0), // Registered Name
        r(1).trim, // Trading Name
        java.util.UUID.randomUUID().toString(),
        nationalId,
        passportId,
        serviceId,
        alienId,
        gender,
        dateOfBirth, // Date of Birth
        dateOfDeath,
        0, // Marital Status
        r(11).trim, // PIN
        0, // Nationality
        r(12).trim, // VAT ID
        "", // TAX
        companyRegNum,
        companyRegDate,
        companyCeaseDate,
        "", // Cease Source
        null, // Number of Dependants
        "", // Place of Birth
        fileOpenDate,
        lastUpdateDate,
        0, // Data Source ID
        "", // Number of Directors
        "", // Number of Shareholders
        "", // Status
        statusDate,
        "", // Group Number
        companyType,
        annualTurnover,
        industryCode,
        false) // IS Deleted
  }

  /**
   * Creates an identification object
   */
  def toPIdentification(r: Array[String], parentCrn: Long, dataSourceId: Int, subscriberId: Int, isCompany: Boolean) = {

    var nationalId = ""
    var passportId = ""
    var alienId = ""
    var serviceId = ""
    var nationalityId = 0
    var companyRegNum = ""
    var dataSourceInt = 0
    var status = ""
    var statusDate: java.sql.Date = null
    var dob: java.sql.Date = null
    var companyRegDate: java.sql.Date = null
    var lastUpdateDate: java.sql.Date = null
    var annualTurnover = ""
    var companyType = ""
    var industry = ""
    var taxId = ""
    var vatId = ""

    if (isCompany == false) {
      if (r(11) equals "001") nationalId = r(12)
      else if (r(11) equals "002") passportId = r(12)
      else if (r(11) equals "003") serviceId = r(12)
      else if (r(11) equals "004") alienId = r(12)
    } else if (r(11) equals "005") companyRegNum = r(12)

    // Nationality DB Lookup
    try {
      if (isCompany == false) nationalityId = lookupNationalityId(r(9).trim)
      else nationalityId = lookupNationalityId(r(5).trim)
    } catch { case e: Exception => }

    // Tradung Status Local Lookup
    try if (isCompany == true) status = props.getProperty("ke.tradingStatus." + r(14).trim)
    catch { case e: Exception => }

    // Annual Turnover Band Local Lookup
    try if (isCompany == true) annualTurnover = props.getProperty("ke.annualTurnoverBand." + r(10).trim.toLowerCase)
    catch { case e: Exception => }

    // Industry Local Lookup
    try if (isCompany == true) industry = props.getProperty("ke.employerIndustryType." + r(9).trim.toLowerCase)
    catch { case e: Exception => }

    // Company Type Local Lookup
    try if (isCompany == true) companyType = props.getProperty("ke.companyType." + r(8).trim.toLowerCase)
    catch { case e: Exception => }

    // Company Reg Number Formatting
    try if (isCompany == true) companyRegNum = r(3).trim
    catch { case e: Exception => }

    // Status Date Formatting
    try if (isCompany == true) statusDate = new java.sql.Date(dateFormat.parse(r(15).trim).getTime)
    catch { case e: Exception => }

    // DOB Formatting
    try if (isCompany == false) dob = new java.sql.Date(dateFormat.parse(r(5).trim).getTime)
    catch { case e: Exception => }

    // Company Registration Date Formatting
    try if (isCompany == true) companyRegDate = new java.sql.Date(dateFormat.parse(r(2).trim).getTime)
    catch { case e: Exception => }

    // Last Update Date Formatting
    lastUpdateDate = new java.sql.Date(new java.util.Date().getTime)

    // Tax ID Formatting
    try if (isCompany == true) taxId = r(11).trim
    catch { case e: Exception => }

    // VAT ID
    try if (isCompany == true) vatId = r(12).trim
    catch { case e: Exception => }

    new PIdentification(
      None,
      parentCrn,
      nationalId,
      taxId: String,
      vatId: String,
      passportId,
      nationalityId,
      alienId,
      serviceId,
      companyRegNum,
      dataSourceId,
      status,
      statusDate,
      dob,
      companyRegDate,
      lastUpdateDate,
      annualTurnover,
      companyType,
      industry)
  }

  /**
   * Creates a persistence account summary object
   */
  def toPAccountSummary(r: Array[String], parentCrn: Long, subscriberId: Int, isCompany: Boolean) = {

    var statusId = 0
    var typeId = 0
    var holderTypeId = 0
    var accountNumber = ""
    val listingDate = new java.sql.Date(new java.util.Date().getTime)
    var currentBalance: Double = 0
    var principalAmount: Double = 0
    var lastPaymentDate: java.sql.Date = null

    // Account Status DB Lookup
    if (isCompany == false) statusId = lookupAccountStatusId(r(55).trim)
    else if (isCompany == true) statusId = lookupAccountStatusId(r(46).trim)

    // Account Product Type DB Lookup
    if (isCompany == false) typeId = lookupAccountTypeId(r(43).trim)
    else if (isCompany == true) typeId = lookupAccountTypeId(r(34).trim)

    // Account Holder Type DB Lookup
    try if (isCompany == true) holderTypeId = lookupAccountHolderTypeId(r(33).trim)
    catch { case e: Exception => }

    // Formatting Current Balance
    try {
      if (isCompany == false) currentBalance = r(49).trim.toDouble
      else if (isCompany == true) currentBalance = r(41).trim.toDouble
    } catch { case e: Exception => }

    // Formatting Principal Amount
    try {
      if (isCompany == false) principalAmount = r(46).trim.toDouble
      else if (isCompany == true) principalAmount = r(38).trim.toDouble
    } catch { case e: Exception => }

    // Formatting Last Payment Date
    try {
      if (isCompany == false) lastPaymentDate = new java.sql.Date(dateFormat.parse(r(64).trim).getTime)
      else if (isCompany == true) lastPaymentDate = new java.sql.Date(dateFormat.parse(r(55).trim).getTime)
    } catch { case e: Exception => }

    // Mundane
    if (isCompany == false) accountNumber = r(7).trim
    else if (isCompany == true) accountNumber = r(7).trim

    new PAccountSummary(
      None,
      parentCrn,
      accountNumber,
      statusId,
      typeId,
      holderTypeId,
      subscriberId,
      currentBalance,
      listingDate,
      principalAmount,
      lastPaymentDate)
  }

  /**
   * Creates a persistence account object
   */
  def toPAccount(r: Array[String], parentCrn: Long, subscriberId: Int,
    isCompany: Boolean, dataSourceId: Int, summaryId: Long) = {

    var sectorId: Long = 0
    var statusId = 0
    var holderTypeId = 0
    var accountTypeId = 0
    var currencyId = 0
    var repaymentTermId = 0
    var securityType = ""
    var dateAccountOpened: java.sql.Date = null
    var dateUpdated: java.sql.Date = null
    var creditLimit = 0.0
    var principalAmount = 0.0
    var repaymentPeriod = ""
    var currentBalance = 0.0
    var overdueBalance = 0.0
    var lastPaymentDate: java.sql.Date = null
    val guid = java.util.UUID.randomUUID.toString()
    var sectorOfActivity = ""
    var finalPaymentDate: java.sql.Date = null
    var accountStatusDate: java.sql.Date = null
    var closureReason = ""
    var pastDueDate: java.sql.Date = null
    var numberOfInstallmentsInArrears = 0
    var numberOfDaysInArrears = 0
    var firstPaymentDate: java.sql.Date = null
    var lastPaymentAmount = 0.0
    var annualTurnoverBand = ""
    var deferredPaymentAmount = 0.0
    var deferredPaymentDate: java.sql.Date = null
    var dueDate: java.sql.Date = null
    var jointAccountIndicator = ""
    var localCurrencyAmount = 0.0
    var accountIndicator = ""
    var accountNumber = ""

    // Sector ID DB Lookup
    try {
      if (isCompany == true) sectorId = lookupSectorId(r(9).trim)
      else if (isCompany == false) sectorId = lookupSectorId(r(34).trim)
    } catch { case e: Exception => }

    // Account Status DB Lookup
    try {
      if (isCompany == false) statusId = lookupAccountStatusId(r(55).trim)
      else if (isCompany == true) statusId = lookupAccountStatusId(r(46).trim)
    } catch { case e: Exception => }

    // Account Product Type DB Lookup
    try {
      if (isCompany == false) accountTypeId = lookupAccountTypeId(r(43).trim)
      else if (isCompany == true) accountTypeId = lookupAccountTypeId(r(34).trim)
    } catch { case e: Exception => }

    // Account Holder Type DB Lookup
    try if (isCompany == true) holderTypeId = lookupAccountHolderTypeId(r(33).trim)
    catch { case e: Exception => }

    // Currency DB Lookup
    try {
      if (isCompany == false) currencyId = lookupCurrencyId(r(47).trim)
      else if (isCompany == true) currencyId = lookupCurrencyId(r(39).trim)
    } catch { case e: Exception => }

    // Payment Term DB Lookup
    try {
      if (isCompany == false) repaymentTermId = lookupRepaymentTerm(r(61).trim)
      else if (isCompany == true) repaymentTermId = lookupRepaymentTerm(r(52).trim)
    } catch { case e: Exception => }

    // Security Type Local Lookup
    try {
      if (isCompany == false) securityType = props.getProperty("ke.securityType." + r(66).trim.toLowerCase)
      else if (isCompany == true) securityType = props.getProperty("ke.securityType." + r(57).trim.toLowerCase)
    } catch { case e: Exception => }

    // Annual Turnover Band Local Lookup
    try
      if (isCompany == true) annualTurnoverBand = props.getProperty("ke.annualTurnoverBand." + r(10).trim.toLowerCase)
    catch { case e: Exception => }

    // Date Account Opened Formatting
    try {
      if (isCompany == false) dateAccountOpened = new java.sql.Date(dateFormat.parse(r(44).trim).getTime)
      else if (isCompany == true) dateAccountOpened = new java.sql.Date(dateFormat.parse(r(35).trim).getTime)
    } catch { case e: Exception => }

    // Date Updated Formatting
    dateUpdated = new java.sql.Date(new java.util.Date().getTime)

    // Principal Amount Formatting
    try {
      if (isCompany == false) principalAmount = r(46).trim.toDouble
      else if (isCompany == true) principalAmount = r(38).trim.toDouble
    } catch { case e: Exception => }

    // Repayment Period Formatting
    try {
      if (isCompany == false) repaymentPeriod = r(58).trim
      else if (isCompany == true) repaymentPeriod = r(49).trim
    } catch { case e: Exception => }

    // Current Balance Formatting
    try {
      if (isCompany == false) currentBalance = r(49).trim.toDouble
      else if (isCompany == true) currentBalance = r(41).trim.toDouble
    } catch { case e: Exception => }

    // Overdue Balance Formatting
    try {
      if (isCompany == false) overdueBalance = r(50).trim.toDouble
      if (isCompany == true) overdueBalance = r(42).trim.toDouble
    } catch { case e: Exception => }

    // Last Payment Date Formatting
    try {
      if (isCompany == false) lastPaymentDate = new java.sql.Date(dateFormat.parse(r(64).trim).getTime)
      else if (isCompany == true) lastPaymentDate = new java.sql.Date(dateFormat.parse(r(55).trim).getTime)
    } catch { case e: Exception => }

    // Sector of Activity Formatting
    if (isCompany == true) sectorOfActivity = r(9).trim

    // Final Payment Date Formatting
    try
      if (isCompany == false) finalPaymentDate = new java.sql.Date(dateFormat.parse(r(64).trim).getTime)
    catch { case e: Exception => }

    // Account Status Date Formatting
    try {
      if (isCompany == false) accountStatusDate = new java.sql.Date(dateFormat.parse(r(56).trim).getTime)
      else if (isCompany == true) accountStatusDate = new java.sql.Date(dateFormat.parse(r(47).trim).getTime)
    } catch { case e: Exception => }

    // Closure Reason Formatting
    try {
      if (isCompany == false) closureReason = r(57).trim
      else if (isCompany == true) closureReason = r(48).trim
    } catch { case e: Exception => }

    // Past Due Date Formatting
    try {
      if (isCompany == false) pastDueDate = new java.sql.Date(dateFormat.parse(r(51).trim).getTime)
      else if (isCompany == true) pastDueDate = new java.sql.Date(dateFormat.parse(r(42).trim).getTime)
    } catch { case e: Exception => }

    // Number of Installments in Arrears Formatting
    try {
      if (isCompany == false) numberOfInstallmentsInArrears = r(53).trim.toInt
      else if (isCompany == true) numberOfInstallmentsInArrears = r(45).trim.toInt
    } catch { case e: Exception => }

    // Number of Days in Arrears Formatting
    try {
      if (isCompany == false) numberOfDaysInArrears = r(52).trim.toInt
      else if (isCompany == true) numberOfDaysInArrears = r(44).trim.toInt
    } catch { case e: Exception => }

    // First Payment Date Formatting
    try {
      if (isCompany == false) firstPaymentDate = new java.sql.Date(dateFormat.parse(r(62).trim).getTime)
      else if (isCompany == true) firstPaymentDate = new java.sql.Date(dateFormat.parse(r(53).trim).getTime)
    } catch { case e: Exception => }

    // First Payment Amount Formatting
    try {
      if (isCompany == false) lastPaymentAmount = r(65).trim.toDouble
      else if (isCompany == true) lastPaymentAmount = r(56).trim.toDouble
    } catch { case e: Exception => }

    // Deferred Payment Amount Formatting
    try {
      if (isCompany == false) deferredPaymentAmount = r(60).trim.toDouble
      if (isCompany == true) deferredPaymentAmount = r(51).trim.toDouble
    } catch { case e: Exception => }

    // Deferred Payment Date Formatting
    try {
      if (isCompany == false) deferredPaymentDate = new java.sql.Date(dateFormat.parse(r(59).trim).getTime)
      else if (isCompany == true) deferredPaymentDate = new java.sql.Date(dateFormat.parse(r(50).trim).getTime)
    } catch { case e: Exception => }

    // Due Date Formatting
    try {
      if (isCompany == false) dueDate = new java.sql.Date(dateFormat.parse(r(45).trim).getTime)
      if (isCompany == true) dueDate = new java.sql.Date(dateFormat.parse(r(36).trim).getTime)
    } catch { case e: Exception => }

    // Joint Account Indicator Formatting
    try {
      if (isCompany == false) jointAccountIndicator = r(42).trim
      else if (isCompany == true) jointAccountIndicator = r(32)
    } catch { case e: Exception => }

    // Local Currency Amount Formatting
    try {
      if (isCompany == false) localCurrencyAmount = r(48).trim.toDouble
      else if (isCompany == true) localCurrencyAmount = r(40).trim.toDouble
    } catch { case e: Exception => }

    // NPL/Account Indicator Formatting
    try {
      if (isCompany == false) accountIndicator = r(54).trim
      else if (isCompany == true) accountIndicator = r(37).trim
    } catch { case e: Exception => }

    // Account Number
    try {
      if (isCompany == false) accountNumber = r(7).trim
      else if (isCompany == true) accountNumber = r(7).trim
    } catch { case e: Exception => }

    new PAccount(
      parentCrn,
      1, // Bank Listing
      subscriberId,
      0, // User ID
      sectorId,
      accountNumber,
      accountTypeId,
      statusId,
      holderTypeId,
      currencyId,
      dateAccountOpened,
      dateUpdated,
      creditLimit,
      principalAmount,
      repaymentPeriod,
      repaymentTermId,
      None, // Scheduled Monthly Repayment
      None, // Actual Monthly Repayment
      None, // Collateral Type
      currentBalance,
      overdueBalance,
      lastPaymentDate,
      dataSourceId,
      guid,
      sectorOfActivity,
      finalPaymentDate,
      holderTypeId,
      accountStatusDate,
      closureReason,
      pastDueDate,
      numberOfInstallmentsInArrears,
      numberOfDaysInArrears,
      None, // Interest Rate
      firstPaymentDate,
      None,
      securityType,
      lastPaymentAmount,
      dateUpdated, // Listing Date
      None, // Exchange Rate
      summaryId,
      annualTurnoverBand,
      deferredPaymentAmount,
      deferredPaymentDate,
      None, // Disbursement Amount
      firstPaymentDate,
      dueDate,
      jointAccountIndicator,
      localCurrencyAmount,
      accountIndicator)
  }
}