include "shared.thrift"

namespace cpp metadataServer
namespace py metadataServer
namespace java metadataServer

/* we can use shared.<datatype>, instead we could also typedef them for
	convenience */
typedef shared.response response
typedef shared.file file
typedef shared.uploadResponse uploadResponse


struct version {
	1: i32 version
}

struct listHash {
	1: list<string> hashList
}

service MetadataServerService {

	file getFile(1: string filename),
	file downloadResponse(1: file hashedFile),
	file download(1: string filename, 2: string metaName),
	uploadResponse upload(1: string filename, 2: list<string> hashList, 3: i32 clientVersion, 4: i32 numberOfM, 5: string metaName),
	uploadResponse storeFile(1: string filename, 2: list<string> hashList, 3: i32 version),
	response checkFileVersion(1: string filename, 2: i32 clientVersion),
	version getVersion(1: string filename),
	listHash getHashList(1: string filename),
	response triggerDelete(1: string filename, 2: i32 numberOfM, 3: string metaName), 
	response deleteFile(1: string filename),
	uploadResponse storeFileResponse(1: list<string> missingList, 2: file f),
	response triggerGossip(1: string metaName)
}
