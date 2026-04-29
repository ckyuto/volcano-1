/*
Copyright 2025 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vgpu

import (
	"testing"
)

func TestCheckGPUtype(t *testing.T) {
	tests := []struct {
		name     string
		annos    map[string]string
		cardtype string
		want     bool
	}{
		{
			name: "Single GPUInUse value matches",
			annos: map[string]string{
				GPUInUse: "NVIDIA",
			},
			cardtype: "NVIDIA",
			want:     true,
		},
		{
			name: "GPUInUse mixed case matches",
			annos: map[string]string{
				GPUInUse: "nvidia",
			},
			cardtype: "NVIDIA",
			want:     true,
		},
		{
			name: "Multiple GPUInUse values, one matches",
			annos: map[string]string{
				GPUInUse: "AMD,NVIDIA,INTEL",
			},
			cardtype: "NVIDIA",
			want:     true,
		},
		{
			name: "Multiple GPUInUse values, none matches",
			annos: map[string]string{
				GPUInUse: "AMD,INTEL,MLU",
			},
			cardtype: "NVIDIA",
			want:     false,
		},
		{
			name: "Single GPUNoUse value matches",
			annos: map[string]string{
				GPUNoUse: "NVIDIA",
			},
			cardtype: "NVIDIA",
			want:     false,
		},
		{
			name: "GPUNoUse mixed case matches",
			annos: map[string]string{
				GPUNoUse: "nvidia",
			},
			cardtype: "NVIDIA",
			want:     false,
		},
		{
			name: "Multiple GPUNoUse values, one matches",
			annos: map[string]string{
				GPUNoUse: "AMD,NVIDIA,INTEL",
			},
			cardtype: "NVIDIA",
			want:     false,
		},
		{
			name: "Multiple GPUNoUse values, none matches",
			annos: map[string]string{
				GPUNoUse: "AMD,INTEL,MLU",
			},
			cardtype: "NVIDIA",
			want:     true,
		},
		{
			name:     "Empty annotations map",
			annos:    map[string]string{},
			cardtype: "NVIDIA",
			want:     true,
		},
		{
			name:     "Nil annotations map",
			annos:    nil,
			cardtype: "NVIDIA",
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := checkGPUtype(tt.annos, tt.cardtype); got != tt.want {
				t.Errorf("checkGPUtype() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGPUScore(t *testing.T) {
	idle := &GPUDevice{UsedNum: 0, UsedMem: 0, Memory: 16384}
	shared := &GPUDevice{UsedNum: 1, UsedMem: 4096, Memory: 16384}
	full := &GPUDevice{UsedNum: 4, UsedMem: 16384, Memory: 16384}

	testCases := []struct {
		name   string
		policy string
		device *GPUDevice
		want   float64
	}{
		{name: "spread rewards idle GPU", policy: spreadPolicy, device: idle, want: spreadMultiplier},
		{name: "spread does not reward shared GPU", policy: spreadPolicy, device: shared, want: 0},
		{name: "spread does not reward full GPU", policy: spreadPolicy, device: full, want: 0},
		{name: "binpack rewards full GPU most", policy: binpackPolicy, device: full, want: binpackMultiplier},
		{name: "binpack does not reward idle GPU", policy: binpackPolicy, device: idle, want: 0},
		{name: "unset policy returns zero", policy: "", device: shared, want: 0},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := GPUScore(tc.policy, tc.device); got != tc.want {
				t.Errorf("GPUScore(%q) = %f, want %f", tc.policy, got, tc.want)
			}
		})
	}
}
