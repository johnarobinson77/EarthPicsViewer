package org.jar;

/*
 * Copyright (c) 2019, John A. Robinson
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors
 *    may be used to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 */

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import javax.swing.*;
import javax.swing.filechooser.FileSystemView;

public class InputForm<messageAppend> extends JPanel {

    private JTextField[] fields;
    private JTextArea textArea = new JTextArea("Messages\n");
    private String appTitle = null;
    private JCheckBox fileCopy = new JCheckBox("Copy JPEG Files");

    // Create a form with the specified labels, tooltips, and sizes.
    //public InputForm() { super(new BorderLayout());}
    public InputForm(String appTitlele) {
        super(new BorderLayout());
        this.appTitle = appTitlele;
    }

    public void buildTextForm(String[] labels, char[] mnemonics, int[] widths, String[] tips, JButton[] buttons) {

        JPanel labelPanel = new JPanel(new GridLayout(labels.length, 1));
        JPanel fieldPanel = new JPanel(new GridLayout(labels.length, 1));
        JPanel buttonPanel = new JPanel(new GridLayout(buttons.length, 1));
        add(labelPanel, BorderLayout.WEST);
        add(fieldPanel, BorderLayout.CENTER);
        add(buttonPanel, BorderLayout.EAST);
        fields = new JTextField[labels.length];

        for (int i = 0; i < labels.length; i += 1) {
            fields[i] = new JTextField();
            if (i < tips.length)
                fields[i].setToolTipText(tips[i]);
            if (i < widths.length)
                fields[i].setColumns(widths[i]);

            JLabel lab = new JLabel(labels[i], JLabel.RIGHT);
            lab.setLabelFor(fields[i]);
            if (i < mnemonics.length)
                lab.setDisplayedMnemonic(mnemonics[i]);

            labelPanel.add(lab);
            JPanel p = new JPanel(new FlowLayout(FlowLayout.LEFT));
            p.add(fields[i]);
            fieldPanel.add(p);
            buttonPanel.add(buttons[i]);
        }
    }

    public String getText(int i) {
        return (fields[i].getText());
    }

    public void myFileChooser(int i, String title){
        // create an object of JFileChooser class
        JFileChooser j = new JFileChooser(FileSystemView.getFileSystemView().getHomeDirectory());
        // set to choose Directories only
        j.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
        // invoke the showsOpenDialog function to show the save dialog
        int r = j.showOpenDialog(null);

        // if the user selects a directory
        if (r == JFileChooser.APPROVE_OPTION) {
            // set the label to the path of the selected file
            fields[i].setText(j.getSelectedFile().getAbsolutePath());
        }
        // if the user cancelled the operation
        else {
            //fields[i].setText("the user cancelled the operation");
        }
    }

    public void start(String[] inputs) {
        System.out.println(inputs[0] + " " + inputs[1] + " " + inputs[2]
                + " " + inputs[3] + " " + inputs[4]);
        messageAppendLn(inputs[0] + " " + inputs[1] + " " + inputs[2]
                + " " + inputs[3] + " " + inputs[4]);
        // Handle user input of the date range
        SimpleDateFormat dateF = null;
        Timestamp afterTime = new Timestamp(0);
        Timestamp beforeTime = new Timestamp(System.currentTimeMillis());
        //Timestamp beforeTime = new Timestamp(Long.MAX_VALUE);
        try {
            if (inputs[3] != null && inputs[3].length() > 0) {
                if (inputs[3].split(" ").length == 1) {
                    dateF = new SimpleDateFormat("MM/dd/yyyy");
                } else if (inputs[3].split(":").length == 1) {
                    dateF = new SimpleDateFormat("MM/dd/yyyy HH");
                } else if (inputs[3].split(":").length == 2) {
                    dateF = new SimpleDateFormat("MM/dd/yyyy HH:mm");
                } else if (inputs[3].split(":").length == 3) {
                    dateF = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                }
                afterTime = new Timestamp(dateF.parse(inputs[3]).getTime());
            }
            if (inputs[4] != null && inputs[4].length() > 0) {
                if (inputs[4].split(" ").length == 1) {
                    dateF = new SimpleDateFormat("MM/dd/yyyy");
                } else if (inputs[4].split(":").length == 1) {
                    dateF = new SimpleDateFormat("MM/dd/yyyy HH");
                } else if (inputs[4].split(":").length == 2) {
                    dateF = new SimpleDateFormat("MM/dd/yyyy HH:mm");
                } else if (inputs[4].split(":").length == 3) {
                    dateF = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                }
                beforeTime = new Timestamp(dateF.parse(inputs[4]).getTime());
            }
        } catch (ParseException e) {
            System.out.println("Earliest or Latest Time not understood");
            messageAppendLn("Earliest or Latest Time not understood");
            return;
        }
    }

    public void messageAppend(String message) {
        textArea.append(message);
        textArea.setCaretPosition(textArea.getText().length());
    }
    public void messageAppendLn(String message) {
        textArea.append(message + "\n");
        textArea.setCaretPosition(textArea.getText().length());
    }

    public static void main(String[] args) {
        InputForm inputForm = new InputForm("Input Form Test");
        inputForm.getUserInput();
    }

    public void getUserInput() {
        final String[] labels = { "Picture Directory", "KML Directory", "Document Title", "Earliest Time", "Latest Time" };
        final int[] widths = { 20, 20, 20, 20, 20 };
        final String[] descs = { "Input Picture Directory",
                "Output KML Directory",
                "Name of KML file and document title",
                "Earliest Time [mm/dd/yyy hh:mm:ss] (Optional)",
                "Latest Time [mm/dd/yyy hh:mm:ss] (Optional)" };
        final char[] mnemonics = {'P', 'K', 'D', 'E', 'L'};

        JButton fromDir = new JButton("choose");
        fromDir.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                myFileChooser(0, labels[0]);
            }
        });

        JButton toDir = new JButton("choose");
        toDir.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                myFileChooser(1, labels[1]);
            }
        });

        JButton documentName = new JButton("default");
        documentName.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                fields[2].setText("Photo Locations");
            }
        });
        JButton clearAfterTime = new JButton("clear");
        clearAfterTime.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                fields[3].setText("");
            }
        });
        JButton clearBeforeTime = new JButton("clear");
        clearBeforeTime.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                fields[4].setText("");
            }
        });

        JButton[] buttons = {fromDir, toDir, documentName, clearAfterTime, clearBeforeTime};
        buildTextForm(labels, mnemonics, widths, descs, buttons);

        JButton submit = new JButton("Go");
        submit.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                String[] inputs = new String[6];
                for (int i = 0; i<5; i++){
                    inputs[i] = getText(i);
                }
                inputs[5] = fileCopy.isSelected() ? "Yes" : "No";
                submit.setEnabled(false);
                start(inputs);  // this is overridden by the calling process.
                submit.setEnabled(true);
            }
        });

        //TEXT AREA
        textArea.setSize(400,400);
        textArea.setRows(4);
        textArea.setLineWrap(true);
        textArea.setEditable(false);
        textArea.setVisible(true);

        JScrollPane scroll = new JScrollPane (textArea);
        scroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
        scroll.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);

        // checkbox
        fileCopy.setToolTipText("Copy JPEG files to output directory");

        JFrame f = new JFrame(appTitle);
        f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        f.getContentPane().add(this, BorderLayout.NORTH);
        JPanel p = new JPanel(new GridLayout(1, 2));
        p.add(fileCopy, BorderLayout.WEST);
        p.add(submit, BorderLayout.EAST);
        f.getContentPane().add(p, BorderLayout.SOUTH);
        f.add(scroll);
        f.pack();
        f.setVisible(true);
    }
}
