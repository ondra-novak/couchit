/*
 * IChangeNotify.h
 *
 *  Created on: 21. 3. 2016
 *      Author: ondra
 */

#ifndef LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_ICHANGENOTIFY_H_
#define LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_ICHANGENOTIFY_H_

namespace LightCouch {


class ChangedDoc;

class IChangeNotify {
public:
	virtual ~IChangeNotify() {}

	///Called when change is notified from the database
	/**
	 * @param changeInfo information about changed document
	 * @param db instance of database client, which can be used to ask additional data
	 * to process the change
	 * @retval true continue listening
	 * @retval false stop listening now
	 */
	virtual bool onChange(const ChangedDoc &changeInfo) throw() = 0;
};

} /* namespace LightCouch */

#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_ICHANGENOTIFY_H_ */
