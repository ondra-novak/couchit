
#pragma once
namespace couchit {

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
	const Enum *find(StrViewA name) const;

	const std::size_t count;
	const NamedEnumDef<Enum> * const items;

	String toString(StrViewA separator = StrViewA(",")) const;

protected:


	template<int n>
	static void orderByName(NamedEnumDef<Enum> (&arr)[n]) {
		unsigned int idxs[n];
		for (unsigned int i = 0; i < n;++i) idxs[i] = i;
		std::sort(idxs, idxs+n, [&](unsigned int a, unsigned int b) {
			return arr[a].name.value.compare(arr[b].name.value) < 0;
		});
		for (unsigned int i = 0; i < n;++i) arr[i].name.order = idxs[i];
	}

	template<int n>
	static void orderByEnum(NamedEnumDef<Enum> (&arr)[n]) {
		unsigned int idxs[n];
		for (unsigned int i = 0; i < n;++i) idxs[i] = i;
		std::sort(idxs, idxs+n, [&](unsigned int a, unsigned int b) {
			return arr[a].enm.value < arr[b].enm.value;
		});
		for (unsigned int i = 0; i < n;++i) arr[i].enm.order = idxs[i];
	}

};

class UnknownEnumName: public std::exception {
public:
	UnknownEnumName(const StrViewA name, const String &set)
		:name(name),set(set) {}

	const String name;
	const String set;

	virtual const char *what() const throw();

	virtual ~UnknownEnumName() throw () {}

protected:
	mutable String whatMsg;



};

template<typename Enum>
template<int n>
inline NamedEnum<Enum>::NamedEnum(Def (&arr)[n]):count(n),items(arr)
{
	orderByName(arr);
	orderByEnum(arr);
}

template<typename Enum>
inline Enum NamedEnum<Enum>::operator [](StrViewA name) const {
	const Enum *r = find(name);
	if (r) return *r;
	else throw UnknownEnumName(name,toString());
}

template<typename Enum>
inline const Enum * NamedEnum<Enum>::find(StrViewA name) const {
	std::size_t h = 0;
	std::size_t t = count;
	while (h < t) {
		std::size_t c = (h + t) / 2;
		std::size_t i = items[c].name.order;
		int cr = name.compare(items[i].name.value);
		if (cr < 0) t = c;
		else if (cr > 0) h = c + 1;
		else {
			return & items[i].enm.value;
		}
	}
	return nullptr;
}


template<typename Enum>
inline StrViewA NamedEnum<Enum>::operator [](Enum enm) const {
	std::size_t h = 0;
	std::size_t t = count;
	while (h < t) {
		std::size_t c= (h + t)/2;
		std::size_t i = items[c].enm.order;
		Enum chl = items[i].enm.value;
		if (enm < chl) t = c;
		else if (enm > chl) h = c+1;
		else return items[i].name.value;
	}
	return StrViewA();

}

template<typename Enum>
inline NamedEnum<Enum>::NamedEnum(std::initializer_list<Def>& arr)
:count(arr.end() - arr.begin()), items(arr.begin())
{
}


template<typename Enum>
String NamedEnum<Enum>::toString(StrViewA separator)const {
	std::ostringstream builder;
	if (count) {
		builder << items[0].name.value;
		for (std::size_t i = 1; i < count;i++) {
			builder << separator << items[i].name.value;
		}
	}
	return String(builder.str());
}


inline const char* UnknownEnumName::what() const throw () {
	if (whatMsg.empty()) {
		whatMsg = String({"Unknown enumeration value: '", name, "'. The value was not found in the following set: '", set, "'."});
	}
	return whatMsg.c_str();
}



}

