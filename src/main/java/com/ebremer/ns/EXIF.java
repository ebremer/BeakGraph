package com.ebremer.ns;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

/**
 * Apache Jena vocabulary constants for Exif data description vocabulary
 * Namespace: http://www.w3.org/2003/12/exif/ns#
 * See: https://www.w3.org/2003/12/exif/
 */
public final class EXIF {
    /** Exif namespace (with trailing #) */
    public static final String NS = "http://www.w3.org/2003/12/exif/ns#";
    public static String getURI() { return NS; }
    private static String uri(String localName) { return NS + localName; }

    // ================================================================
    // Classes
    // ================================================================
    public static final Resource IFD = ResourceFactory.createResource(uri("IFD"));

    // ================================================================
    // Properties
    // ================================================================
    public static final Property gpsTimeStamp = ResourceFactory.createProperty(NS, "gpsTimeStamp");
    public static final Property exposureTime = ResourceFactory.createProperty(NS, "exposureTime");
    public static final Property maxApertureValue = ResourceFactory.createProperty(NS, "maxApertureValue");
    public static final Property artist = ResourceFactory.createProperty(NS, "artist");
    public static final Property yCbCrCoefficients = ResourceFactory.createProperty(NS, "yCbCrCoefficients");
    public static final Property focalLengthIn35mmFilm = ResourceFactory.createProperty(NS, "focalLengthIn35mmFilm");
    public static final Property primaryChromaticities = ResourceFactory.createProperty(NS, "primaryChromaticities");
    public static final Property gpsInfo_IFD_Pointer = ResourceFactory.createProperty(NS, "gpsInfo_IFD_Pointer");
    public static final Property gpsDifferential = ResourceFactory.createProperty(NS, "gpsDifferential");
    public static final Property rowsPerStrip = ResourceFactory.createProperty(NS, "rowsPerStrip");
    public static final Property bitsPerSample = ResourceFactory.createProperty(NS, "bitsPerSample");
    public static final Property gpsInfo = ResourceFactory.createProperty(NS, "gpsInfo");
    public static final Property userInfo = ResourceFactory.createProperty(NS, "userInfo");
    public static final Property componentsConfiguration = ResourceFactory.createProperty(NS, "componentsConfiguration");
    public static final Property gpsMeasureMode = ResourceFactory.createProperty(NS, "gpsMeasureMode");
    public static final Property pimInfo = ResourceFactory.createProperty(NS, "pimInfo");
    public static final Property sharpness = ResourceFactory.createProperty(NS, "sharpness");
    public static final Property pimColorBalance = ResourceFactory.createProperty(NS, "pimColorBalance");
    public static final Property flashpixVersion = ResourceFactory.createProperty(NS, "flashpixVersion");
    public static final Property gpsDestDistanceRef = ResourceFactory.createProperty(NS, "gpsDestDistanceRef");
    public static final Property exposureProgram = ResourceFactory.createProperty(NS, "exposureProgram");
    public static final Property subjectDistance = ResourceFactory.createProperty(NS, "subjectDistance");
    public static final Property gpsTrack = ResourceFactory.createProperty(NS, "gpsTrack");
    public static final Property gpsImgDirection = ResourceFactory.createProperty(NS, "gpsImgDirection");
    public static final Property userComment = ResourceFactory.createProperty(NS, "userComment");
    public static final Property datatype = ResourceFactory.createProperty(NS, "datatype");
    public static final Property gpsSpeedRef = ResourceFactory.createProperty(NS, "gpsSpeedRef");
    public static final Property gpsVersionID = ResourceFactory.createProperty(NS, "gpsVersionID");
    public static final Property exifdata = ResourceFactory.createProperty(NS, "exifdata");
    public static final Property gpsDestBearing = ResourceFactory.createProperty(NS, "gpsDestBearing");
    public static final Property gpsImgDirectionRef = ResourceFactory.createProperty(NS, "gpsImgDirectionRef");
    public static final Property gpsTrackRef = ResourceFactory.createProperty(NS, "gpsTrackRef");
    public static final Property sceneCaptureType = ResourceFactory.createProperty(NS, "sceneCaptureType");
    public static final Property subjectArea = ResourceFactory.createProperty(NS, "subjectArea");
    public static final Property gpsLongitude = ResourceFactory.createProperty(NS, "gpsLongitude");
    public static final Property spatialFrequencyResponse = ResourceFactory.createProperty(NS, "spatialFrequencyResponse");
    public static final Property contrast = ResourceFactory.createProperty(NS, "contrast");
    public static final Property tag_number = ResourceFactory.createProperty(NS, "tag_number");
    public static final Property subSecTimeOriginal = ResourceFactory.createProperty(NS, "subSecTimeOriginal");
    public static final Property planarConfiguration = ResourceFactory.createProperty(NS, "planarConfiguration");
    public static final Property oecf = ResourceFactory.createProperty(NS, "oecf");
    public static final Property model = ResourceFactory.createProperty(NS, "model");
    public static final Property yResolution = ResourceFactory.createProperty(NS, "yResolution");
    public static final Property meteringMode = ResourceFactory.createProperty(NS, "meteringMode");
    public static final Property subjectLocation = ResourceFactory.createProperty(NS, "subjectLocation");
    public static final Property photometricInterpretation = ResourceFactory.createProperty(NS, "photometricInterpretation");
    public static final Property gainControl = ResourceFactory.createProperty(NS, "gainControl");
    public static final Property gpsDateStamp = ResourceFactory.createProperty(NS, "gpsDateStamp");
    public static final Property pimBrightness = ResourceFactory.createProperty(NS, "pimBrightness");
    public static final Property stripByteCounts = ResourceFactory.createProperty(NS, "stripByteCounts");
    public static final Property exif_IFD_Pointer = ResourceFactory.createProperty(NS, "exif_IFD_Pointer");
    public static final Property relatedImageFileFormat = ResourceFactory.createProperty(NS, "relatedImageFileFormat");
    public static final Property whiteBalance = ResourceFactory.createProperty(NS, "whiteBalance");
    public static final Property referenceBlackWhite = ResourceFactory.createProperty(NS, "referenceBlackWhite");
    public static final Property ifdPointer = ResourceFactory.createProperty(NS, "ifdPointer");
    public static final Property exifVersion = ResourceFactory.createProperty(NS, "exifVersion");
    public static final Property brightnessValue = ResourceFactory.createProperty(NS, "brightnessValue");
    public static final Property relatedFile = ResourceFactory.createProperty(NS, "relatedFile");
    public static final Property pimSharpness = ResourceFactory.createProperty(NS, "pimSharpness");
    public static final Property shutterSpeedValue = ResourceFactory.createProperty(NS, "shutterSpeedValue");
    public static final Property stripOffsets = ResourceFactory.createProperty(NS, "stripOffsets");
    public static final Property meter = ResourceFactory.createProperty(NS, "meter");
    public static final Property imageConfig = ResourceFactory.createProperty(NS, "imageConfig");
    public static final Property fileSource = ResourceFactory.createProperty(NS, "fileSource");
    public static final Property pixelYDimension = ResourceFactory.createProperty(NS, "pixelYDimension");
    public static final Property orientation = ResourceFactory.createProperty(NS, "orientation");
    public static final Property gpsStatus = ResourceFactory.createProperty(NS, "gpsStatus");
    public static final Property imageLength = ResourceFactory.createProperty(NS, "imageLength");
    public static final Property make = ResourceFactory.createProperty(NS, "make");
    public static final Property xResolution = ResourceFactory.createProperty(NS, "xResolution");
    public static final Property gpsSatellites = ResourceFactory.createProperty(NS, "gpsSatellites");
    public static final Property copyright = ResourceFactory.createProperty(NS, "copyright");
    public static final Property relatedImageLength = ResourceFactory.createProperty(NS, "relatedImageLength");
    public static final Property exifAttribute = ResourceFactory.createProperty(NS, "exifAttribute");
    public static final Property subSecTimeDigitized = ResourceFactory.createProperty(NS, "subSecTimeDigitized");
    public static final Property focalPlaneXResolution = ResourceFactory.createProperty(NS, "focalPlaneXResolution");
    public static final Property versionInfo = ResourceFactory.createProperty(NS, "versionInfo");
    public static final Property tagid = ResourceFactory.createProperty(NS, "tagid");
    public static final Property relatedImageWidth = ResourceFactory.createProperty(NS, "relatedImageWidth");
    public static final Property gpsSpeed = ResourceFactory.createProperty(NS, "gpsSpeed");
    public static final Property seconds = ResourceFactory.createProperty(NS, "seconds");
    public static final Property gpsDestLatitude = ResourceFactory.createProperty(NS, "gpsDestLatitude");
    public static final Property focalPlaneYResolution = ResourceFactory.createProperty(NS, "focalPlaneYResolution");
    public static final Property cfaPattern = ResourceFactory.createProperty(NS, "cfaPattern");
    public static final Property width = ResourceFactory.createProperty(NS, "width");
    public static final Property software = ResourceFactory.createProperty(NS, "software");
    public static final Property gpsAreaInformation = ResourceFactory.createProperty(NS, "gpsAreaInformation");
    public static final Property flash = ResourceFactory.createProperty(NS, "flash");
    public static final Property geo = ResourceFactory.createProperty(NS, "geo");
    public static final Property exposureIndex = ResourceFactory.createProperty(NS, "exposureIndex");
    public static final Property whitePoint = ResourceFactory.createProperty(NS, "whitePoint");
    public static final Property pimSaturation = ResourceFactory.createProperty(NS, "pimSaturation");
    public static final Property recOffset = ResourceFactory.createProperty(NS, "recOffset");
    public static final Property imageDescription = ResourceFactory.createProperty(NS, "imageDescription");
    public static final Property gpsLatitudeRef = ResourceFactory.createProperty(NS, "gpsLatitudeRef");
    public static final Property imageUniqueID = ResourceFactory.createProperty(NS, "imageUniqueID");
    public static final Property compression = ResourceFactory.createProperty(NS, "compression");
    public static final Property subjectDistanceRange = ResourceFactory.createProperty(NS, "subjectDistanceRange");
    public static final Property printImageMatching_IFD_Pointer = ResourceFactory.createProperty(NS, "printImageMatching_IFD_Pointer");
    public static final Property interoperability_IFD_Pointer = ResourceFactory.createProperty(NS, "interoperability_IFD_Pointer");
    public static final Property flashEnergy = ResourceFactory.createProperty(NS, "flashEnergy");
    public static final Property gpsDestLatitudeRef = ResourceFactory.createProperty(NS, "gpsDestLatitudeRef");
    public static final Property isoSpeedRatings = ResourceFactory.createProperty(NS, "isoSpeedRatings");
    public static final Property subSecTime = ResourceFactory.createProperty(NS, "subSecTime");
    public static final Property spectralSensitivity = ResourceFactory.createProperty(NS, "spectralSensitivity");
    public static final Property gpsDestLongitude = ResourceFactory.createProperty(NS, "gpsDestLongitude");
    public static final Property date = ResourceFactory.createProperty(NS, "date");
    public static final Property gpsDestLongitudeRef = ResourceFactory.createProperty(NS, "gpsDestLongitudeRef");
    public static final Property interopInfo = ResourceFactory.createProperty(NS, "interopInfo");
    public static final Property gpsDOP = ResourceFactory.createProperty(NS, "gpsDOP");
    public static final Property resolutionUnit = ResourceFactory.createProperty(NS, "resolutionUnit");
    public static final Property sceneType = ResourceFactory.createProperty(NS, "sceneType");
    public static final Property saturation = ResourceFactory.createProperty(NS, "saturation");
    public static final Property lightSource = ResourceFactory.createProperty(NS, "lightSource");
    public static final Property focalPlaneResolutionUnit = ResourceFactory.createProperty(NS, "focalPlaneResolutionUnit");
    public static final Property pixelXDimension = ResourceFactory.createProperty(NS, "pixelXDimension");
    public static final Property samplesPerPixel = ResourceFactory.createProperty(NS, "samplesPerPixel");
    public static final Property pimContrast = ResourceFactory.createProperty(NS, "pimContrast");
    public static final Property exposureMode = ResourceFactory.createProperty(NS, "exposureMode");
    public static final Property customRendered = ResourceFactory.createProperty(NS, "customRendered");
    public static final Property gpsAltitudeRef = ResourceFactory.createProperty(NS, "gpsAltitudeRef");
    public static final Property subseconds = ResourceFactory.createProperty(NS, "subseconds");
    public static final Property gpsMapDatum = ResourceFactory.createProperty(NS, "gpsMapDatum");
    public static final Property yCbCrPositioning = ResourceFactory.createProperty(NS, "yCbCrPositioning");
    public static final Property imageDataStruct = ResourceFactory.createProperty(NS, "imageDataStruct");
    public static final Property imageWidth = ResourceFactory.createProperty(NS, "imageWidth");
    public static final Property dateTimeOriginal = ResourceFactory.createProperty(NS, "dateTimeOriginal");
    public static final Property yCbCrSubSampling = ResourceFactory.createProperty(NS, "yCbCrSubSampling");
    public static final Property gpsProcessingMethod = ResourceFactory.createProperty(NS, "gpsProcessingMethod");
    public static final Property imageDataCharacter = ResourceFactory.createProperty(NS, "imageDataCharacter");
    public static final Property fNumber = ResourceFactory.createProperty(NS, "fNumber");
    public static final Property interoperabilityVersion = ResourceFactory.createProperty(NS, "interoperabilityVersion");
    public static final Property jpegInterchangeFormat = ResourceFactory.createProperty(NS, "jpegInterchangeFormat");
    public static final Property gpsAltitude = ResourceFactory.createProperty(NS, "gpsAltitude");
    public static final Property exposureBiasValue = ResourceFactory.createProperty(NS, "exposureBiasValue");
    public static final Property apertureValue = ResourceFactory.createProperty(NS, "apertureValue");
    public static final Property relatedSoundFile = ResourceFactory.createProperty(NS, "relatedSoundFile");
    public static final Property dateTime = ResourceFactory.createProperty(NS, "dateTime");
    public static final Property resolution = ResourceFactory.createProperty(NS, "resolution");
    public static final Property gpsLatitude = ResourceFactory.createProperty(NS, "gpsLatitude");
    public static final Property makerNote = ResourceFactory.createProperty(NS, "makerNote");
    public static final Property length = ResourceFactory.createProperty(NS, "length");
    public static final Property height = ResourceFactory.createProperty(NS, "height");
    public static final Property jpegInterchangeFormatLength = ResourceFactory.createProperty(NS, "jpegInterchangeFormatLength");
    public static final Property compressedBitsPerPixel = ResourceFactory.createProperty(NS, "compressedBitsPerPixel");
    public static final Property dateAndOrTime = ResourceFactory.createProperty(NS, "dateAndOrTime");
    public static final Property deviceSettingDescription = ResourceFactory.createProperty(NS, "deviceSettingDescription");
    public static final Property dateTimeDigitized = ResourceFactory.createProperty(NS, "dateTimeDigitized");
    public static final Property colorSpace = ResourceFactory.createProperty(NS, "colorSpace");
    public static final Property transferFunction = ResourceFactory.createProperty(NS, "transferFunction");
    public static final Property interoperabilityIndex = ResourceFactory.createProperty(NS, "interoperabilityIndex");
    public static final Property mm = ResourceFactory.createProperty(NS, "mm");
    public static final Property gpsDestDistance = ResourceFactory.createProperty(NS, "gpsDestDistance");
    public static final Property pictTaking = ResourceFactory.createProperty(NS, "pictTaking");
    public static final Property _unknown = ResourceFactory.createProperty(NS, "_unknown");
    public static final Property gpsDestBearingRef = ResourceFactory.createProperty(NS, "gpsDestBearingRef");
    public static final Property digitalZoomRatio = ResourceFactory.createProperty(NS, "digitalZoomRatio");
    public static final Property sensingMethod = ResourceFactory.createProperty(NS, "sensingMethod");
    public static final Property focalLength = ResourceFactory.createProperty(NS, "focalLength");
    public static final Property gpsLongitudeRef = ResourceFactory.createProperty(NS, "gpsLongitudeRef");

    private EXIF() {}
}
