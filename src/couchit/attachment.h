/*
 * attachment.h
 *
 *  Created on: 19. 6. 2016
 *      Author: ondra
 */

#ifndef LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_ATTACHMENT_H_
#define LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_ATTACHMENT_H_

#include "string.h"

#include "json.h"
namespace couchit {


///Contains attachment data as reference
/** use this class to refer some binary data (with proper content type). Data
 * are not stored with the object, you must not destroy referred objects
 */
class AttachmentDataRef: public BinaryView {
public:
	///Constructor
	/**
	 *
	 * @param data reference to binary data
	 * @param contenType content type
	 */
	AttachmentDataRef(const BinaryView &data, const StrViewA &contentType)
		:BinaryView(data),contentType(contentType) {}
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
	AttachmentData(const String &data, const String &contentType)
		:AttachmentDataRef(BinaryView(StrViewA(data)),StrViewA(contentType)),bindata(data),ctx(contentType) {}
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
	String bindata;
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
		virtual void operator()(json::BinaryView) = 0;
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

		///Reads next buffer
		/***
		 * @return read buffer, if empty, eof is reached
		 */
		BinaryView read() {
			if (put_back_content.empty())return impl_read();
			else {
				BinaryView ret = put_back_content;
				put_back_content = BinaryView();
				return ret;
			}
		}

		///Puts back a part of unprocessed data
		/**
		 * Data put back are read by next reading operation
		 *
		 * @param put_back data to put back
		 */
		void putBack(const BinaryView &put_back) {
			put_back_content = put_back;
		};


	protected:
		BinaryView put_back_content;

		///Read buffer from the stream
		/** Returns buffer with at least one byte, however it will
		 * mostly return more bytes.
		 *
		 * @return a view which contains read bytes. If empty view is
		 * returned, then EOF has been reached
		 *
		 * @note this function doesn't process putBack data.
		 */
		virtual BinaryView impl_read() = 0;
};


	///Contans content type
	const String contentType;

	///Contains e-tag
	const String etag;

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

	json::Binary download();

	void putBack(const BinaryView &b) {
		sptr->putBack(b);
	}

	BinaryView read() {
		return sptr->read();
	}

	///Load whole attachment into memory
	std::vector<unsigned char> load() {
		std::vector<unsigned char> buffer;
		buffer.resize(length);
		this->read(buffer.data(),length);
		return buffer;
	}

	///Read to buffer
	/**
	 * @param buffer pointer to buffer
	 * @param size size of buffer
	 * @return actually read bytes. Zero returned is EOF
	 *
	 */
	std::size_t read(void *buffer, std::size_t size) {
		BinaryView x = read();
		if (size > x.length) size = x.length;
		std::copy(x.data, x.data+size, reinterpret_cast<unsigned char *>(buffer));
		return size;
	}
	///Read from stream
	/**
	 * Deprecated.
	 *
	 * Function reads data from the stream.
	 * @param processed count of bytes processed during previous read
	 * @return buffer including unprocessed bytes
	 *
	 */
	[[deprecated]]
	BinaryView read(std::size_t processed) {
		BinaryView x;
		if (processed == 0) {
			x = read();
			putBack(x);
			return x;
		} else {
			x = read();
			x = x.substr(processed);
			putBack(x);
		}
		return x;
	}

	Value json();


private:

	RefCntPtr<Source> sptr;

};

}


#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_ATTACHMENT_H_ */
