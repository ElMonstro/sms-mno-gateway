package config

import (
	"testing"
)

func TestGetEnvAsQueueWeights(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		defaults map[string]int
		want     map[string]int
	}{
		{
			name:     "parses valid pairs",
			input:    "QUEUE_A:50,QUEUE_B:100",
			defaults: nil,
			want:     map[string]int{"QUEUE_A": 50, "QUEUE_B": 100},
		},
		{
			name:     "trims whitespace",
			input:    " QUEUE_A : 50 , QUEUE_B : 100 ",
			defaults: nil,
			want:     map[string]int{"QUEUE_A": 50, "QUEUE_B": 100},
		},
		{
			name:     "single queue",
			input:    "SMS_MNO_GATEWAY_QUEUE:200",
			defaults: nil,
			want:     map[string]int{"SMS_MNO_GATEWAY_QUEUE": 200},
		},
		{
			name:     "empty string falls back to defaults",
			input:    "",
			defaults: map[string]int{"DEFAULT_QUEUE": 1},
			want:     map[string]int{"DEFAULT_QUEUE": 1},
		},
		{
			name:     "invalid weight skipped, valid pair kept",
			input:    "QUEUE_A:notanumber,QUEUE_B:10",
			defaults: nil,
			want:     map[string]int{"QUEUE_B": 10},
		},
		{
			name:     "all invalid falls back to defaults",
			input:    "QUEUE_A:bad",
			defaults: map[string]int{"DEFAULT_QUEUE": 1},
			want:     map[string]int{"DEFAULT_QUEUE": 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TEST_QUEUE_WEIGHTS", tt.input)
			got := getEnvAsQueueWeights("TEST_QUEUE_WEIGHTS", tt.defaults)
			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			for k, v := range tt.want {
				if got[k] != v {
					t.Errorf("key %q: got %d, want %d", k, got[k], v)
				}
			}
		})
	}
}

func TestLoad_SDPBatchSizes(t *testing.T) {
	t.Setenv("QUEUE_SDP_BATCH_SIZES", "SMS_MNO_GATEWAY_QUEUE:50,TITANIC-KE_SMS_QUEUE:100")
	cfg := Load()
	if cfg.Queues.SDPBatchSizes["SMS_MNO_GATEWAY_QUEUE"] != 50 {
		t.Errorf("SMS_MNO_GATEWAY_QUEUE batch size: got %d, want 50", cfg.Queues.SDPBatchSizes["SMS_MNO_GATEWAY_QUEUE"])
	}
	if cfg.Queues.SDPBatchSizes["TITANIC-KE_SMS_QUEUE"] != 100 {
		t.Errorf("TITANIC-KE_SMS_QUEUE batch size: got %d, want 100", cfg.Queues.SDPBatchSizes["TITANIC-KE_SMS_QUEUE"])
	}
}

func TestLoad_SDPBatchSizes_Empty(t *testing.T) {
	t.Setenv("QUEUE_SDP_BATCH_SIZES", "")
	cfg := Load()
	if cfg.Queues.SDPBatchSizes != nil {
		t.Errorf("expected nil SDPBatchSizes when env is empty, got %v", cfg.Queues.SDPBatchSizes)
	}
}
