package main

import (
	"fmt"
	"strings"
)

func main() {
	lorem := `
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed non risus. 
Suspendisse lectus tortor, dignissim sit amet, adipiscing nec, ultricies 
sed, dolor. Cras elementum ultrices diam. Maecenas ligula massa, varius 
a, semper congue, euismod non, mi.
`
	var b strings.Builder
	fmt.Println("start")
	for b.Len() < 32761 {
		b.WriteString(lorem)
		b.WriteString(" ")
	}
	payload := b.String()[:32761]
	fmt.Println(payload)
	fmt.Println("end")
}
