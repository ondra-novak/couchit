
#pragma once
namespace LightCouch {

namespace _details {
	template<typename T>
	struct NamedEnumFieldOrder {
		T value;
		unsigned int order;

		NamedEnumFieldOrder(const T &v):value(v) {}
	};
	template<>
	struct NamedEnumFieldOrder<StrViewA> {
		StrViewA value;
		unsigned int order;

		NamedEnumFieldOrder(const StrViewA &v):value(v) {}
		NamedEnumFieldOrder(const char *v):value(v) {}
	};
}


template<typename Enum>
struct NamedEnumDef {
	///enum specification
	_details::NamedEnumFieldOrder<Enum> enm;
	///associated name
	_details::NamedEnumFieldOrder<StrViewA> name;
};


template<typename Enum>
class NamedEnum {
public:

	typedef NamedEnumDef<Enum> Def;

	template<int n>
	NamedEnum(Def (&arr)[n]);

	NamedEnum(std::initializer_list<Def> &arr);

	Enum operator[](StrViewA name) const;
	StrViewA operator[](Enum enm) const;
	Enum *find(StrViewA name) const;

	const std::size_t count;
	const NamedEnumDef<Enum> * const items;

	String toString(StrViewA separator = StrViewA(",")) const;

protected:


	template<int n>
	static void orderByName(NamedEnumDef<Enum> (&arr)[n]) {
		unsigned int idxs[n];
		for (unsigned int i = 0; i < n;++i) idxs[i] = i;
		std::sort(idxs, idxs+n, [&](unsigned int a, unsigned int b) {
			return items[a].name.value.compare(items[b].name.value) < 0;
		});
		for (unsigned int i = 0; i < n;++i) items[i].name.order = idxs[i];
	}

	template<int n>
	static void orderByEnum(NamedEnumDef<Enum> (&arr)[n]) {
		unsigned int idxs[n];
		for (unsigned int i = 0; i < n;++i) idxs[i] = i;
		std::sort(idxs, idxs+n, [&](unsigned int a, unsigned int b) {
			return items[a].enm.value < items[b].enm.value;
		});
		for (unsigned int i = 0; i < n;++i) items[i].enm.order = idxs[i];
	}

};

class UnknownEnumName: public std::exception {
public:
	UnknownEnumName(const StrView name, const String &set)
		:name(name),set(set) {}

	const String name;
	const String set;

	virtual const char *what() const throw() = 0;

	virtual ~UnknownEnumName() throw () {}

protected:
	mutable String whatMsg;



};



	template<typenae Enum>
	UnknownEnumName(const ProgramLocation &loc, StrViewA name, const NamedEnum<Enum> * const items);
	virtual ~UnknownEnumName() throw () {}

	StrViewA getName() const;
	StrViewA getSet() const;

protected:
	StringA name;
	StringA set;

	void message(ExceptionMsg &msg) const;

};


template<typename Enum>
template<int n>
inline NamedEnum<Enum>::NamedEnum(Def (&arr)[n]):count(n),items(arr)
{
	orderByName(arr);
	orderByEnum(arr);
}

template<typename Enum>
inline Enum LightSpeed::NamedEnum<Enum>::operator [](StrViewA name) const {
	Enum *r = find(name)
	if (r) return *r;
	else throw UnknownEnumName(THISLOCATION,name,this);
}

template<typename Enum>
inline bool LightSpeed::NamedEnum<Enum>::find(StrViewA name, Enum &res) const {
	std::size_t h = 0;
	std::size_t t = count;
	while (h < t) {
		std::size_t c = (h + t) / 2;
		std::size_t i = items[c].name.order;
		CompareResult cr = name.compare(items[i].name.value);
		if (cr == cmpResultLess) t = c;
		else if (cr == cmpResultGreater) h = c + 1;
		else {
			res = items[i].enm.value;
			return true;
		}
	}
	return false;
}


template<typename Enum>
inline StrViewA NamedEnum<Enum>::operator [](Enum enm) const {
	std::size_t h = 0;
	std::size_t t = count;
	while (h < t) {
		std::size_t rc= (h + t)/2;
		std::size_t i = items[c].enm.order;
		Enum chl = items[i].enm.value;
		if (enm < chl) t = c;
		else if (enm > chl) h = c+1;
		else return items[i].name.value;
	}
	return StrViewA();

}



template<typename Enum>
StringA NamedEnum<Enum>::toString(StrViewA separator)const {
	AutoArray<char, SmallAlloc<256> > setValues;
	if (count) {
		setValues.append(items[0].name.value);
		for (std::size_t i = 1; i < count;i++) {
			setValues.append(separator);
			setValues.append(items[i].name.value);
		}
	}
	return setValues;
}

template<typename Enum>
inline UnknownEnumName::UnknownEnumName(const ProgramLocation& loc,
		StrViewA name, const NamedEnum<Enum>* const items)
	:Exception(loc),name(name),set(items->toString())
{


}




}



