#pragma once

namespace couchit {

template<typename T>
class IterRange {
public:
	IterRange(std::pair<T,T> &&range):range(std::move(range)) {}
	IterRange(T &&lower, T &&upper):range(std::move(lower),std::move(upper)) {}
	const T &begin() const {return range.first;}
	const T &end() const {return range.second;}

protected:
	std::pair<T,T> range;
};

template<typename T>
IterRange<T> iterRange(std::pair<T,T> &&range) {return IterRange<T>(std::move(range));}
template<typename T>
IterRange<T> iterRange(T &&lower, T &&upper) {return IterRange<T>(std::move(lower),std::move(upper));}


}
