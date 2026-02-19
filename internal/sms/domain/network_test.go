package domain

import "testing"

func TestParseNetwork(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Network
	}{
		// Valid networks
		{name: "SAFARICOM uppercase", input: "SAFARICOM", expected: NetworkSafaricom},
		{name: "safaricom lowercase", input: "safaricom", expected: NetworkSafaricom},
		{name: "Safaricom mixed case", input: "Safaricom", expected: NetworkSafaricom},
		{name: "AIRTEL uppercase", input: "AIRTEL", expected: NetworkAirtel},
		{name: "airtel lowercase", input: "airtel", expected: NetworkAirtel},
		{name: "TELKOM uppercase", input: "TELKOM", expected: NetworkTelkom},
		{name: "telkom lowercase", input: "telkom", expected: NetworkTelkom},
		{name: "EQUITEL uppercase", input: "EQUITEL", expected: NetworkEquitel},
		{name: "equitel lowercase", input: "equitel", expected: NetworkEquitel},
		{name: "CM uppercase", input: "CM", expected: NetworkCM},
		{name: "cm lowercase", input: "cm", expected: NetworkCM},
		{name: "INTNL uppercase", input: "INTNL", expected: NetworkINTNL},
		{name: "intnl lowercase", input: "intnl", expected: NetworkINTNL},

		// With whitespace
		{name: "with leading space", input: "  SAFARICOM", expected: NetworkSafaricom},
		{name: "with trailing space", input: "SAFARICOM  ", expected: NetworkSafaricom},
		{name: "with both spaces", input: "  SAFARICOM  ", expected: NetworkSafaricom},

		// Unknown networks
		{name: "unknown VODAFONE", input: "VODAFONE", expected: NetworkUnknown},
		{name: "unknown MTN", input: "MTN", expected: NetworkUnknown},
		{name: "empty string", input: "", expected: NetworkUnknown},
		{name: "random string", input: "xyz123", expected: NetworkUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseNetwork(tt.input)
			if result != tt.expected {
				t.Errorf("ParseNetwork(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestNetwork_String(t *testing.T) {
	tests := []struct {
		network  Network
		expected string
	}{
		{NetworkSafaricom, "SAFARICOM"},
		{NetworkAirtel, "AIRTEL"},
		{NetworkTelkom, "TELKOM"},
		{NetworkEquitel, "EQUITEL"},
		{NetworkCM, "CM"},
		{NetworkINTNL, "INTNL"},
		{NetworkUnknown, "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.network.String()
			if result != tt.expected {
				t.Errorf("String() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestNetwork_IsValid(t *testing.T) {
	tests := []struct {
		network  Network
		expected bool
	}{
		{NetworkSafaricom, true},
		{NetworkAirtel, true},
		{NetworkTelkom, true},
		{NetworkEquitel, true},
		{NetworkCM, true},
		{NetworkINTNL, true},
		{NetworkUnknown, false},
	}

	for _, tt := range tests {
		t.Run(tt.network.String(), func(t *testing.T) {
			result := tt.network.IsValid()
			if result != tt.expected {
				t.Errorf("IsValid() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestNetwork_IsSafaricom(t *testing.T) {
	tests := []struct {
		network  Network
		expected bool
	}{
		{NetworkSafaricom, true},
		{NetworkAirtel, false},
		{NetworkTelkom, false},
		{NetworkEquitel, false},
		{NetworkCM, false},
		{NetworkINTNL, false},
		{NetworkUnknown, false},
	}

	for _, tt := range tests {
		t.Run(tt.network.String(), func(t *testing.T) {
			result := tt.network.IsSafaricom()
			if result != tt.expected {
				t.Errorf("IsSafaricom() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestNetwork_IsInternational(t *testing.T) {
	tests := []struct {
		network  Network
		expected bool
	}{
		{NetworkSafaricom, false},
		{NetworkAirtel, false},
		{NetworkTelkom, false},
		{NetworkEquitel, false},
		{NetworkCM, true},
		{NetworkINTNL, true},
		{NetworkUnknown, false},
	}

	for _, tt := range tests {
		t.Run(tt.network.String(), func(t *testing.T) {
			result := tt.network.IsInternational()
			if result != tt.expected {
				t.Errorf("IsInternational() = %v, want %v", result, tt.expected)
			}
		})
	}
}
