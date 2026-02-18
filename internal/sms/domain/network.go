package domain

import "strings"

// Network represents a mobile network operator
type Network string

const (
	NetworkSafaricom Network = "SAFARICOM"
	NetworkAirtel    Network = "AIRTEL"
	NetworkTelkom    Network = "TELKOM"
	NetworkEquitel   Network = "EQUITEL"
	NetworkCM        Network = "CM"
	NetworkINTNL     Network = "INTNL"
	NetworkUnknown   Network = "UNKNOWN"
)

// ParseNetwork converts a string to a Network type
func ParseNetwork(s string) Network {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "SAFARICOM":
		return NetworkSafaricom
	case "AIRTEL":
		return NetworkAirtel
	case "TELKOM":
		return NetworkTelkom
	case "EQUITEL":
		return NetworkEquitel
	case "CM":
		return NetworkCM
	case "INTNL":
		return NetworkINTNL
	default:
		return NetworkUnknown
	}
}

// String returns the string representation of the network
func (n Network) String() string {
	return string(n)
}

// IsValid checks if the network is a known network
func (n Network) IsValid() bool {
	return n != NetworkUnknown
}

// IsSafaricom returns true if the network is Safaricom
func (n Network) IsSafaricom() bool {
	return n == NetworkSafaricom
}

// IsInternational returns true if the network is CM or INTNL
func (n Network) IsInternational() bool {
	return n == NetworkCM || n == NetworkINTNL
}
