/*
 * attachment.h
 *
 *  Created on: 19. 6. 2016
 *      Author: ondra
 */

#ifndef LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_ATTACHMENT_H_
#define LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_ATTACHMENT_H_
#include "lightspeed/base/containers/string.h"

#include "string.h"

#include "json.h"
namespace LightCouch {

using namespace LightSpeed;

///Contains attachment data as reference
/** use this class to refer some binary data (with proper content type). Data
 * are not stored with the object, you must not destroy referred objects
 */
class AttachmentDataRef: public ConstBin {
public:
	///Constructor
	/**
	 *
	 * @param data reference to binary data
	 * @param contenType content type
	 */
	AttachmentDataRef(const ConstBin data, const StrViewA &contentType)
		:ConstBin(data),contentType(contentType) {}
	const StrViewA contentType;

	///Converts data to base64 string.
	String toBase64() const;

	Value toInline() const;

};

class Download;
///Contains attachment data
/** use this class to store some binary data with proper content type. Data
 * are stored with the object. This may result to some extra memory allocation.
 *
 * Because StringB is used, you can prepare the binary data into this object. If you
 * pass StringB as argument then no copying is made, object is shared instead.
 */
class AttachmentData: public AttachmentDataRef {
public:

	///Constructor
	/**
	 * @param data binary data
	 * @param contentType content type
	 */
	AttachmentData(const StringB &data, const String &contentType)
		:AttachmentDataRef(data,StrViewA(contentType)),bindata(data),ctx(contentType) {}
	///Constructor from JSON value
	/**
	 * @param attachment value contains attachment from a document. It required that
	 * attachment is in-lined, you cannot construct object from a stub.
	 */
	AttachmentData(const Value &attachment);


	///Converts download object to AttachmentData;
	AttachmentData(Download &&dwn);

	///Creates attachment from base64 string
	/**
	 * @param base64 base64 string
	 * @param contentType contenr type
	 * @return attachment object
	 */
	static AttachmentData fromBase64(const StrViewA &base64, const StrViewA &contentType);

private:
	StringB bindata;
	String ctx;
};

///The class helps to upload content to the database
/** The class is used to upload attachments. You can get the instance by calling the function
 * CouchDB::uploadAttachment();
 *
 * To feed the object by a data, use the function Upload::write(). Once you put
 * all data there, call finish() and the function returns document's revision ID.
 */
class Upload {
public:

	class Target: public RefCntObj {
	public:
		virtual ~Target() noexcept(false) {}
		virtual void operator()(const void *buffer, std::size_t size) = 0;
		virtual String finish() = 0;
	};


	Target &write;

	String finish();

	Upload(Target *t);

private:

	RefCntPtr<Target> tptr;
};

///The class helps to download content from the database
class Download {
public:

	class Source: public RefCntObj {
	public:
		virtual ~Source() {}
		virtual natural operator()(void *buffer, std::size_t size) = 0;
	};

	///Reads data from the stream
	/**
	 * @param buffer a prepared buffer by the caller
	 * @param size size of prepared buffer, must be at least one byte large
	 * @return count of bytes read. Function always reads at-least one byte. If
	 *   zero is returned, then end of file has been extracted.
	 */
	Source &read;

	///Contans content type
	const String contentType;

	///Contains e-tag
	const String 	etag;

	///Contains length
	const std::size_t length;

	///Contains true, if content has not been modified
	/**
	 * If notModified is true, the variables read,contentTyp and length are undefined.
	 * To use the feature 'notModified' you need to supply previous etag while downloading the attachment
	 */
	const bool notModified;

	Download(Source *s,
			const String contentType,
			const String 	etag,
			const std::size_t length,
			const bool notModified);



private:

	RefCntPtr<Source> sptr;

};

}


#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_ATTACHMENT_H_ */
