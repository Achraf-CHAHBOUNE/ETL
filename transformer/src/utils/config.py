import re
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Database connection parameters
SOURCE_DB_HOST = os.getenv("SOURCE_MYSQL_HOST")
SOURCE_DB_USER = os.getenv("SOURCE_MYSQL_USER")
SOURCE_DB_PASSWORD = os.getenv("SOURCE_MYSQL_PASSWORD")
SOURCE_DB_NAME = os.getenv("SOURCE_MYSQL_DB")
SOURCE_DB_PORT = 3306
# SOURCE_DB_PORT = int(os.getenv("SOURCE_MYSQL_PORT", default=3306))

DEST_DB_HOST = os.getenv("DEST_MYSQL_HOST")
DEST_DB_USER = os.getenv("DEST_MYSQL_USER")
DEST_DB_PASSWORD = os.getenv("DEST_MYSQL_PASSWORD")
DEST_DB_PORT = int(os.getenv("DEST_MYSQL_PORT", default=3306))
DEST_DB_NAME_5MIN = os.getenv("DEST_MYSQL_DB_5MIN")
DEST_DB_NAME_15MIN = os.getenv("DEST_MYSQL_DB_15MIN")
DEST_DB_NAME_MGW = os.getenv("DEST_MYSQL_DB_MGW")

# Source Database config
SOURCE_DB_CONFIG = {
    'host': SOURCE_DB_HOST,
    'user': SOURCE_DB_USER,
    'password': SOURCE_DB_PASSWORD,
    'port': SOURCE_DB_PORT,
    'database': SOURCE_DB_NAME
}

# 5min Destination Database config
DEST_DB_CONFIG_5MIN = {
    'host': DEST_DB_HOST,
    'user': DEST_DB_USER,
    'password': DEST_DB_PASSWORD,
    'port': DEST_DB_PORT,
    'database': DEST_DB_NAME_5MIN
}

# 15min Destination Database config
DEST_DB_CONFIG_15MIN = {
    'host': DEST_DB_HOST,
    'user': DEST_DB_USER,
    'password': DEST_DB_PASSWORD,
    'port': DEST_DB_PORT,
    'database': DEST_DB_NAME_15MIN
}

# MGW Destination Database config
DEST_DB_CONFIG_MGW = {
    'host': DEST_DB_HOST,
    'user': DEST_DB_USER,
    'password': DEST_DB_PASSWORD,
    'port': DEST_DB_PORT,
    'database': DEST_DB_NAME_MGW
}

# Node patterns
NOEUD_PATTERN = re.compile(r'^(CALIS|MEIND|RAIND)', re.IGNORECASE)
NOEUD_PATTERN_MGW = re.compile(r'^(MGW)', re.IGNORECASE)

# Files config
FILES_PATHS = {
    '5min': './data/our_data/result_5min.txt',
    '15min': './data/our_data/result_15min.txt',
    'mgw': './data/our_data/result_mgw.txt',
    'last_extracted': './data/last_extracted.json'
}

# Suffix to operator mapping
SUFFIX_OPERATOR_MAPPING = {
    'nw': 'Inwi',
    'mt': 'Maroc Telecom',
    'ie': 'International',
    'is': 'International',
    'bs': 'Orange 2G',
    'be': 'Orange 2G',
    'ne': 'Orange 3G',
    'ns': 'Orange 3G'
}

# KPI families
KPI_FAMILIES_5MIN = {
    # 'traffic': [
    #     'TRAF_Erlang_S', 'TRAF_Erlang_E', 'TRAF_RDT', 'TRAF_CircHS', 
    #     'TRAF_ALOC_E', 'TRAF_ALOC_S', 'TRAF_FCS'
    # ],
    # 'ASR': [
    #     'ASR_S', 'ASR_E', 'ASR_IN', 'ASR_OUT'
    # ],
    # 'Success': [
    #     'Success_SIP_IN', 'Success_SIP_OUT', 'Succ_VoIP_Seiz_Attempts'
    # ],
    # 'RouteUtilization': [
    #     'RouteUtilizationIn', 'RouteUtilizationOut'
    # ],
    # 'ALOC': [
    #     'ALOC_IN', 'ALOC_OUT'
    # ],
    # 'CSFB': [
    #     'CSFB_MT_Eff', 'CSFB_Call_MT', 'CSFB_Paging'
    # ],
    # 'SGS': [
    #     'SGS_UpdateLocation', 'SGS_SMS_MO', 'SGS_SMS_MT'
    # ],
    # 'SGSLA': [
    #     'SGSLA_Attach_Reg', 'SGSLA_Attach_NonReg', 
    #     'SGSLA_LocUpdate_Reg', 'SGSLA_LocUpdate_NonReg'
    # ],
    # 'Resource': [
    #     'CPU'
    # ]
}

KPI_FAMILIES_15MIN = {
    # 'Paging': ['TxPaging']
}

KPI_FAMILIES_MGW = {
    # 'Quality': [
    #     'RateOfLowJitterStream', 'LatePktsRatio', 'NoDisturbJitter',
    #     'IPQoS', 'PktLoss'
    # ],
    # 'Traffic': [
    #     'UseOfLicence', 'MediaStreamChannelUtilisationRate',
    #     'ReceivedBwLink1WithHeaders', 'ReceivedBwLink2WithHeaders',
    #     'TransBwLink1WithHeaders', 'TransBwLink2WithHeaders', 'TotalBwForSig'
    # ],
    # 'Success': [
    #     'IPBCPestablishSuccessRate', 'IPTerminationSuccessRate'
    # ],
    # 'Errors': [
    #     'IPInDiscards', 'IPOutDiscards'
    # ]
}

# KPI formulas for 5min data
KPI_FORMULAS_5MIN = {
    "CPU": {
        "numerator": ["LoasACCLOAD"],
        "denominator": ["LoasNSCAN"],
        "Suffix": False,
        "family": "Resource",
        "formula": lambda num, denom: (sum(num) / sum(denom)) if sum(denom) != 0 else None
    },
    "SGS_UpdateLocation": {
        "numerator": ["SgsNSLOCREGSGS"],
        "denominator": ["SgsNTLOCREGSGS"],
        "Suffix": False,
        "family": "SGS",
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "SGS_SMS_MO": {
        "numerator": ["SgsNSMOSMS"],
        "denominator": ["SgsNTMOSMS"],
        "Suffix": False,
        "family": "SGS",
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "SGS_SMS_MT": {
        "numerator": ["SgsNSMTSMS"],
        "denominator": ["SgsNTMTSMS"],
        "Suffix": False,
        "family": "SGS",
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TxPaging1": {
        "numerator": ["LocNLAPAG1RESUCC", "LocNLAPAG2RESUCC"],
        "denominator": ["LocNLAPAG1LOTOT"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TxMajLa": {
        "numerator": ["LocNLALOCSUCC"],
        "denominator": ["LocNLALOCTOT"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TxCall_OC": {
        "numerator": ["ChasNCHAFRMSUCC", "ChasNMSFRMSCCI"],
        "denominator": ["ChasNCHAFRMTOT", "ChasNMSFRMTOTI"],
        "Suffix": False,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TxCall_TC": {
        "numerator": ["ChasNCHATOMSUCC", "ChasNMSTOMSCCO"],
        "denominator": ["ChasNCHATOMTOT", "ChasNMSTOMTOTO"],
        "Suffix": False,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "EffAuthen_HLR": {
        "numerator": ["SecNAUTFTCSUCC"],
        "denominator": ["SecNAUTFTCTOT"],
        "Suffix": False,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "Eff_RABASN_In": {
        "numerator": ["RncNRNFRMSCCI"],
        "denominator": ["RncNRNFRMTOTI"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "Eff_RABASN_Out": {
        "numerator": ["RncNRNTOMSCCO"],
        "denominator": ["RncNRNTOMTOTO"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TxHORNCOut": {
        "numerator": ["RncNRNTORGSUCC"],
        "denominator": ["RncNRNTRRRGTOT"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TxHOBSCOut": {
        "numerator": ["BscNBSTOHBSUCC"],
        "denominator": ["BscNBSTRHRTOT"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TxHOBSCIn": {
        "numerator": ["BscNBSTIHBSUCC", "BscNBSTIUGHBSUCC"],
        "denominator": ["BscNBSTSHRTOT", "BscNBSTSUGHRTOT"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TxSms_MO": {
        "numerator": ["ShmNSMSCAOSUCC"],
        "denominator": ["ShmNSMSRDOTOT"],
        "Suffix": False,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TxSms_MT": {
        "numerator": ["ShmNSMSSRSUCC"],
        "denominator": ["ShmNSMSSMRLTOT"],
        "Suffix": False,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "TRAF_Erlang_S": {
        "numerator": ["TrunkrouteNTRALACCO"],
        "denominator": ["TrunkrouteNSCAN"],
        "Suffix": True,
        "family": "traffic",
        "formula": lambda num, denom: (sum(num) / sum(denom)) if sum(denom) != 0 else None
    },
    "TRAF_Erlang_E": {
        "numerator": ["TrunkrouteNTRALACCI"],
        "denominator": ["TrunkrouteNSCAN"],
        "Suffix": True,
        "family": "traffic",
        "formula": lambda num, denom: (sum(num) / sum(denom)) if sum(denom) != 0 else None
    },
    "TRAF_RDT": {
        "numerator": ["TrunkrouteNTRALACCO", "TrunkrouteNTRALACCI"],
        "denominator": ["TrunkrouteNSCAN"],
        "additional": ["TrunkrouteNDEV", "TrunkrouteNBLOCACC"],
        "Suffix": True,
        "family": "traffic",
        "formula": lambda num, denom, add: ((sum(num) / sum(denom)) / (add[0] - (add[1] / sum(denom)))) * 100 if (sum(denom) != 0 and (add[0] - (add[1] / sum(denom))) != 0) else None
    },
    "TRAF_CircHS": {
        "numerator": ["TrunkrouteNBLOCACC"],
        "denominator": ["TrunkrouteNSCAN", "TrunkrouteNDEV"],
        "Suffix": True,
        "family": "traffic",
        "formula": lambda num, denom: (sum(num) / denom[0]) / denom[1] * 100 if (denom[0] != 0 and denom[1] != 0) else None
    },
    "TRAF_ALOC_E": {
        "numerator": ["TrunkrouteNTRALACCI"],
        "denominator": ["TrunkrouteNSCAN", "TrunkrouteNANSWERSI"],
        "Suffix": True,
        "family": "traffic",
        "formula": lambda num, denom: (sum(num) / denom[0]) / denom[1] * 300 if (denom[0] != 0 and denom[1] != 0) else None
    },
    "TRAF_ALOC_S": {
        "numerator": ["TrunkrouteNTRALACCO"],
        "denominator": ["TrunkrouteNSCAN", "TrunkrouteNANSWERSO"],
        "Suffix": True,
        "family": "traffic",
        "formula": lambda num, denom: (sum(num) / denom[0]) / denom[1] * 300 if (denom[0] != 0 and denom[1] != 0) else None
    },
    "TRAF_FCS": {
        "numerator": ["TrunkrouteNBLOCACC"],
        "denominator": ["TrunkrouteNSCAN"],
        "additional": ["TrunkrouteNDEV"],
        "Suffix": True,
        "family": "traffic",
        "formula": lambda num, denom, add: add[0] - (sum(num) / sum(denom)) if sum(denom) != 0 else None
    },
    "ASR_S": {
        "numerator": ["TrunkrouteNANSWERSO"],
        "denominator": ["TrunkrouteNCALLSO", "TrunkrouteNOVERFLOWO"],
        "Suffix": True,
        "family": "ASR",
        "formula": lambda num, denom: (sum(num) / (denom[0] - denom[1])) * 100 if (denom[0] - denom[1]) != 0 else None
    },
    "ASR_E": {
        "numerator": ["TrunkrouteNANSWERSI"],
        "denominator": ["TrunkrouteNCALLSI"],
        "Suffix": True,
        "family": "ASR",
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "RouteUtilizationIn": {
        "numerator": ["VoiproITRALAC"],
        "denominator": ["VoiproNTRAFIND_STASIPI"],
        "Suffix": True,
        "family": "RouteUtilization",
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "RouteUtilizationOut": {
        "numerator": ["VoiproOTRALAC"],
        "denominator": ["VoiproNTRAFIND_STASIPO"],
        "Suffix": True,
        "family": "RouteUtilization",
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "Succ_VoIP_Seiz_Attempts": {
        "numerator": ["VoiproIOVERFL"],
        "Suffix": True,
        "family": "Success",
        "formula": lambda num: (1 - sum(num)) * 100
    },
    "ASR_IN": {
        "numerator": ["VoiproIANSWER"],
        "denominator": ["VoiproNCALLSI"],
        "Suffix": True,
        "family": "ASR",
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "ASR_OUT": {
        "numerator": ["VoiproOANSWER"],
        "denominator": ["VoiproNCALLSO"],
        "Suffix": True,
        "family": "ASR",
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "Success_SIP_IN": {
        "numerator": ["SiproISUCSES"],
        "denominator": ["SiproISIPSES"],
        "Suffix": True,
        "family": "Success",
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "Success_SIP_OUT": {
        "numerator": ["SiproOSUCSES"],
        "denominator": ["SiproOSIPSES"],
        "Suffix": True,
        "family": "Success",
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "Invite_Req_Succ_Ratio": {
        "numerator": ["SipnodNUSINVITE"],
        "denominator": ["SipnodNRINVITE"],
        "Suffix": False,
        "formula": lambda num, denom: (1 - (sum(num) / sum(denom))) * 100 if sum(denom) != 0 else None
    },
    "Rec_SIP_Req_Succ_Ratio": {
        "numerator": ["SipnodONSIPRES"],
        "denominator": ["SipnodINSIPREQ"],
        "Suffix": False,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "Sent_SIP_Req_Succ_Ratio": {
        "numerator": ["SipnodINSIPRES"],
        "denominator": ["SipnodONSIPREQ"],
        "Suffix": False,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "ALOC_IN": {
        "numerator": ["VoiproITRALAC"],
        "denominator": ["VoiproNSCAN", "VoiproIANSWER"],
        "Suffix": True,
        "family": "ALOC",
        "formula": lambda num, denom: (sum(num) / denom[0]) / denom[1] * 300 if (denom[0] != 0 and denom[1] != 0) else None
    },
    "ALOC_OUT": {
        "numerator": ["VoiproOTRALAC"],
        "denominator": ["VoiproNSCAN", "VoiproOANSWER"],
        "Suffix": True,
        "family": "ALOC",
        "formula": lambda num, denom: (sum(num) / denom[0]) / denom[1] * 300 if (denom[0] != 0 and denom[1] != 0) else None
    },
    "CSFB_MT_Eff": {
        "numerator": ["CsfbNSUCCCSFB"],
        "denominator": ["CsfbNSPAG1CSFB", "CsfbNSPAG2CSFB"],
        "Suffix": False,
        "family": "CSFB",
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "CSFB_Call_MT": {
        "numerator": ["CsfsbNSUCCCSFB"],
        "denominator": ["CsfbNSUCCCSFB", "CsfbNUNSUCCCSFB", "CsfbNUSREJCSFB"],
        "Suffix": False,
        "family": "CSFB",
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "CSFB_Paging": {
        "numerator": ["CsfbNSPAG1CSFB", "CsfbNSPAG2CSFB"],
        "denominator": ["CsfbNTPAG1CSFB"],
        "Suffix": False,
        "family": "CSFB",
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "SGSLA_Attach_Reg": {
        "numerator": ["SgslaNSLAATREGSGS"],
        "denominator": ["SgslaNTLAATREGSGS"],
        "Suffix": True,
        "family": "SGSLA",
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "SGSLA_Attach_NonReg": {
        "numerator": ["SgslaNSLAATNREGSGS"],
        "denominator": ["SgslaNTLAATNREGSGS"],
        "Suffix": True,
        "family": "SGSLA",
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "SGSLA_LocUpdate_Reg": {
        "numerator": ["SgslaNSLANLREGSGS"],
        "denominator": ["SgslaNTLANLREGSGS"],
        "Suffix": True,
        "family": "SGSLA",
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "SGSLA_LocUpdate_NonReg": {
        "numerator": ["SgslaNSLANLNREGSGS"],
        "denominator": ["SgslaNTLANLNREGSGS"],
        "Suffix": True,
        "family": "SGSLA",
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    }
}

# KPI formulas for 15min data
KPI_FORMULAS_15MIN = {
    "TxPaging": {
        "numerator": [
            "PagNPAG1REUSUCC", "PagNPAG1RESUCC",
            "PagNPAG2RESUCC", "PagNPAG2REUSUCC"
        ],
        "denominator": [
            "PagNPAG1LOTOT", "PagNPAG1GLTOT", "PagNPAG1LOUTOT"
        ],
        "Suffix": True,
        "family": "Paging",
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    }
}

# KPI formulas for MGW data
KPI_FORMULAS_MGW = {
    "RateOfLowJitterStream": {
        "numerator": [
            "pmVoIpConnMeasuredJitter4", "pmVoIpConnMeasuredJitter5",
            "pmVoIpConnMeasuredJitter6", "pmVoIpConnMeasuredJitter7",
            "pmVoIpConnMeasuredJitter8"
        ],
        "denominator": [
            "pmVoIpConnMeasuredJitter0", "pmVoIpConnMeasuredJitter1",
            "pmVoIpConnMeasuredJitter2", "pmVoIpConnMeasuredJitter3",
            "pmVoIpConnMeasuredJitter4", "pmVoIpConnMeasuredJitter5",
            "pmVoIpConnMeasuredJitter6", "pmVoIpConnMeasuredJitter7",
            "pmVoIpConnMeasuredJitter8"
        ],
        "Suffix": True,
        "formula": lambda num, denom: (1 - sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "LatePktsRatio": {
        "numerator": [
            "pmVoIpConnLatePktsRatio4", "pmVoIpConnLatePktsRatio5",
            "pmVoIpConnLatePktsRatio6"
        ],
        "denominator": [
            "pmVoIpConnLatePktsRatio0", "pmVoIpConnLatePktsRatio1",
            "pmVoIpConnLatePktsRatio2", "pmVoIpConnLatePktsRatio3",
            "pmVoIpConnLatePktsRatio4", "pmVoIpConnLatePktsRatio5",
            "pmVoIpConnLatePktsRatio6"
        ],
        "Suffix": True,
        "formula": lambda num, denom: (1 - sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "NoDisturbJitter": {
        "numerator": ["pmIpCnConnMeasuredJitter5"],
        "denominator": [
            "pmIpCnConnMeasuredJitter0", "pmIpCnConnMeasuredJitter1",
            "pmIpCnConnMeasuredJitter2", "pmIpCnConnMeasuredJitter3",
            "pmIpCnConnMeasuredJitter4", "pmIpCnConnMeasuredJitter5"
        ],
        "Suffix": True,
        "formula": lambda num, denom: (1 - sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "IPQoS": {
        "numerator": [
            "pmIpInDiscards", "pmIpOutDiscards", "pmIpInAddrErrors",
            "pmIpInHdrErrors", "pmIpInUnknownProtos"
        ],
        "denominator": ["pmIpInReceives", "pmIpOutRequests"],
        "Suffix": True,
        "formula": lambda num, denom: (1 - sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "PktLoss": {
        "numerator": ["pmRtpDiscardedPkts", "pmRtpLostPkts"],
        "denominator": ["pmRtpReceivedPktsHi", "pmRtpReceivedPktsLo", "pmRtpLostPkts"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / (denom[0] * 2147483648 + denom[1] + denom[2])) * 100 if (denom[0] * 2147483648 + denom[1] + denom[2]) != 0 else None
    },
    "UseOfLicence": {
        "numerator": ["pmNrOfMeStChUsedVoip"],
        "denominator": ["maxNrOfLicMediaStreamChannelsVoip"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "MediaStreamChannelUtilisationRate": {
        "numerator": ["pmNrOfMediaStreamChannelsBusy"],
        "denominator": ["maxNrOfLicMediaStreamChannels"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "ReceivedBwLink1WithHeaders": {
        "numerator": ["pmIfInOctetsLink1Hi", "pmIfInOctetsLink1Lo"],
        "Suffix": True,
        "formula": lambda num: ((num[0] * 2147483648 + num[1]) / (1000000 * 900)) * 8
    },
    "ReceivedBwLink2WithHeaders": {
        "numerator": ["pmIfInOctetsLink2Hi", "pmIfInOctetsLink2Lo"],
        "Suffix": True,
        "formula": lambda num: ((num[0] * 2147483648 + num[1]) / (1000000 * 900)) * 8
    },
    "TransBwLink1WithHeaders": {
        "numerator": ["pmIfOutOctetsLink1Hi", "pmIfOutOctetsLink1Lo"],
        "Suffix": True,
        "formula": lambda num: ((num[0] * 2147483648 + num[1]) / (1000000 * 900)) * 8
    },
    "TransBwLink2WithHeaders": {
        "numerator": ["pmIfOutOctetsLink2Hi", "pmIfOutOctetsLink2Lo"],
        "Suffix": True,
        "formula": lambda num: ((num[0] * 2147483648 + num[1]) / (1000000 * 900)) * 8
    },
    "TotalBwForSig": {
        "numerator": ["pmSctpStatSentChunks", "pmSctpStatRetransChunks"],
        "Suffix": True,
        "formula": lambda num: (sum(num) / (1000000 * 900)) * 8 * 100 * 1.2
    },
    "IPBCPestablishSuccessRate": {
        "numerator": [
            "pmNrOfRecIpbcpRejectMsg", "pmNrOfSentIpbcpRejectMsg",
            "pmNrOfRecIpbcpConfusedMsg", "pmNrOfRecFaultyIpbcpAcceptMsg",
            "pmNrOfOrigIpbcpBearSupervTmrExp", "pmNrOfTermIpbcpBearSupervTmrExp",
            "pmNrOfRecBctpProtocolFailures"
        ],
        "denominator": ["pmNrOfIpTermsReq", "pmNrOfIpTermsRej"],
        "Suffix": True,
        "formula": lambda num, denom: (1 - sum(num) / (denom[0] - denom[1])) * 100 if (denom[0] - denom[1]) != 0 else None
    },
    "IPTerminationSuccessRate": {
        "numerator": ["pmNrOfIpTermsRej"],
        "denominator": ["pmNrOfIpTermsReq"],
        "Suffix": True,
        "formula": lambda num, denom: (1 - sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "IPInDiscards": {
        "numerator": ["pmIfStatsIpInDiscards"],
        "denominator": ["pmIfStatsIpInReceives"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "IPOutDiscards": {
        "numerator": ["pmIfStatsIpOutDiscards"],
        "denominator": ["pmIfStatsIpOutRequests"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "pmRtpReceivedPkts": {
        "numerator": ["pmRtpReceivedPktsHi", "pmRtpReceivedPktsLo"],
        "Suffix": True,
        "formula": lambda num: (num[0] * 2147483648 + num[1])
    },
    "NbIPTermination": {
        "numerator": ["pmNrOfIpTermsReq", "pmNrOfIpTermsRej"],
        "Suffix": True,
        "formula": lambda num: (num[0] - num[1])
    },
    "LatePktsVoIp": {
        "numerator": ["pmLatePktsVoIp"],
        "denominator": ["pmLatePktsVoIp", "pmSuccTransmittedPktsVoIp"],
        "Suffix": True,
        "formula": lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
    },
    "Pb_IpDatagrams": {
        "numerator": [
            "pmNoOfHdrErrors", "pmNoOfIpAddrErrors",
            "pmNoOfIpInDiscards", "pmNoOfIpOutDiscards"
        ],
        "Suffix": True,
        "formula": lambda num: sum(num)
    }
}

# Configuration dictionary
CONFIGS = {
    '5min': {
        'source_db_config': SOURCE_DB_CONFIG,
        'dest_db_config': DEST_DB_CONFIG_5MIN,
        'kpi_formulas': KPI_FORMULAS_5MIN,
        'kpi_families': KPI_FAMILIES_5MIN,
        'node_pattern': NOEUD_PATTERN,
        'suffix_operator_mapping': SUFFIX_OPERATOR_MAPPING,
        'file_path': FILES_PATHS['5min']
    },
    '15min': {
        'source_db_config': SOURCE_DB_CONFIG,
        'dest_db_config': DEST_DB_CONFIG_15MIN,
        'kpi_formulas': KPI_FORMULAS_15MIN,
        'kpi_families': KPI_FAMILIES_15MIN,
        'node_pattern': NOEUD_PATTERN,
        'suffix_operator_mapping': SUFFIX_OPERATOR_MAPPING,
        'file_path': FILES_PATHS['15min']
    },
    'mgw': {
        'source_db_config': SOURCE_DB_CONFIG,
        'dest_db_config': DEST_DB_CONFIG_MGW,
        'kpi_formulas': KPI_FORMULAS_MGW,
        'kpi_families': KPI_FAMILIES_MGW,
        'node_pattern': NOEUD_PATTERN_MGW,
        'suffix_operator_mapping': SUFFIX_OPERATOR_MAPPING,
        'file_path': FILES_PATHS['mgw']
    }
}