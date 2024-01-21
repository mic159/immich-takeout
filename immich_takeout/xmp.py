from datetime import datetime
import uuid


def make_xmp_sidecar(
    original_timestamp: datetime, gps_coordinates: tuple[float, float] | None
) -> bytes:
    guid = uuid.uuid4().hex.encode()
    bom = b"\xef\xbb\xbf"
    return (
        b'<?xpacket begin="%s" id="%s"?>' % (bom, guid)
        + b'<x:xmpmeta xmlns:x="adobe:ns:meta/">'
        + b'<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'
        + bytes(rdf)
        + b"</rdf:RDF>"
        + b"</x:xmpmeta>"
        + b'<?xpacket end="w"?>'
    )


def build_xml_gps(gps: tuple[float, float]):
    return (
        b"<rdf:Description rdf:about='' xmlns:exif='http://ns.adobe.com/exif/1.0/'>"
        + b"<exif:GPSLatitude>"
        + str(gps[0]).encode("utf-8")
        + b"</exif:GPSLatitude>"
        + b"<exif:GPSLongitude>"
        + str(gps[1]).encode("utf-8")
        + b"</exif:GPSLongitude>"
        + b"</rdf:Description>"
    )
