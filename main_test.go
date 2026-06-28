package main

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt"
)

func signHS256(t *testing.T, secret []byte, c *jwt.StandardClaims) string {
	t.Helper()
	s, err := jwt.NewWithClaims(jwt.SigningMethodHS256, c).SignedString(secret)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return s
}

func TestValidToken(t *testing.T) {
	jwtSecret = []byte("test-secret")
	location = "gke.cluster-test"

	// good builds a fresh, fully-valid claim set each call so tests can mutate
	// one field in isolation.
	good := func() *jwt.StandardClaims {
		return &jwt.StandardClaims{
			Issuer:    "deploys/api",
			Audience:  "deploys/log:" + location,
			Subject:   "user@example.com",
			ExpiresAt: time.Now().Add(time.Hour).Unix(),
		}
	}

	// Happy path: a correctly-signed token with the right issuer/audience/subject
	// is accepted and returns its subject.
	if sub, ok := validToken(signHS256(t, jwtSecret, good())); !ok || sub != "user@example.com" {
		t.Fatalf("valid token: sub=%q ok=%v; want user@example.com,true", sub, ok)
	}

	with := func(mut func(*jwt.StandardClaims)) string {
		c := good()
		mut(c)
		return signHS256(t, jwtSecret, c)
	}

	rejected := []struct {
		name  string
		token string
	}{
		{"empty", ""},
		{"whitespace only", "   "},
		{"malformed", "not.a.jwt"},
		{"wrong secret", signHS256(t, []byte("other-secret"), good())},
		{"wrong issuer", with(func(c *jwt.StandardClaims) { c.Issuer = "evil" })},
		{"wrong audience", with(func(c *jwt.StandardClaims) { c.Audience = "deploys/log:other" })},
		{"empty subject", with(func(c *jwt.StandardClaims) { c.Subject = "" })},
		{"expired", with(func(c *jwt.StandardClaims) { c.ExpiresAt = time.Now().Add(-time.Hour).Unix() })},
	}
	for _, tt := range rejected {
		if sub, ok := validToken(tt.token); ok {
			t.Errorf("%s: token accepted (sub=%q); want rejected", tt.name, sub)
		}
	}

	// Algorithm-confusion defence: a token signed with "none" must be rejected
	// even though its claims are otherwise valid, because the keyfunc requires an
	// HMAC signing method.
	noneTok, err := jwt.NewWithClaims(jwt.SigningMethodNone, good()).
		SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatalf("sign none: %v", err)
	}
	if sub, ok := validToken(noneTok); ok {
		t.Errorf("none-signed token accepted (sub=%q); want rejected", sub)
	}
}

func TestParseTailLines(t *testing.T) {
	tests := []struct {
		in   string
		want int64
	}{
		{"1", 1},
		{"1000", 1000},
		{"5000", maxTailLines},               // exactly the cap
		{"5001", maxTailLines},               // above the cap -> clamped
		{"0", 1000},                          // non-positive -> default
		{"-5", 1000},                         // negative -> default
		{"", 1000},                           // empty -> default
		{"abc", 1000},                        // non-numeric -> default
		{"99999999999999999999999999", 1000}, // overflow -> parse error -> default
	}
	for _, tt := range tests {
		if got := parseTailLines(tt.in); got != tt.want {
			t.Errorf("parseTailLines(%q) = %d; want %d", tt.in, got, tt.want)
		}
	}
}
